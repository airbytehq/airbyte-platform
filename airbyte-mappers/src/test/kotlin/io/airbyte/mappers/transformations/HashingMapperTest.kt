/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.transformations

import TEST_OBJECT_MAPPER
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.module.kotlin.contains
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.Field
import io.airbyte.config.FieldType
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.adapters.TestRecordAdapter
import io.airbyte.config.mapper.configs.HashingConfig
import io.airbyte.config.mapper.configs.HashingMapperConfig
import io.airbyte.config.mapper.configs.HashingMethods
import io.airbyte.mappers.transformations.HashingMapper.Companion.supportedMethods
import io.mockk.spyk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.io.File
import java.security.Security

private const val MAPPER_NAME = "hashing"

class HashingMapperTest {
  private val hashingMapper = spyk(HashingMapper(TEST_OBJECT_MAPPER))

  @Test
  fun schemaTransformsFieldNamesCorrectly() {
    val config =
      HashingMapperConfig(
        MAPPER_NAME,
        null,
        HashingConfig(
          "field1",
          HashingMethods.fromValue("SHA-256")!!,
          "_hashed",
        ),
      )
    val slimStream = SlimStream(listOf(Field("field1", FieldType.DATE), Field("field2", FieldType.DATE)))
    val result = hashingMapper.schema(config, slimStream)
    assertEquals(listOf(Field("field1_hashed", FieldType.STRING), Field("field2", FieldType.DATE)), result.fields)
  }

  @Test
  fun schemaTransformsFieldNamesCorrectlyIfMissingSuffix() {
    val config =
      HashingMapperConfig(
        MAPPER_NAME,
        null,
        HashingConfig(
          "field1",
          HashingMethods.fromValue("SHA-256")!!,
          "_hashed",
        ),
      )
    val slimStream = SlimStream(listOf(Field("field1", FieldType.DATE), Field("field2", FieldType.DATE)))
    val result = hashingMapper.schema(config, slimStream)
    assertEquals(listOf(Field("field1_hashed", FieldType.STRING), Field("field2", FieldType.DATE)), result.fields)
  }

  @ParameterizedTest
  @ValueSource(
    strings = [
      HashingMapper.MD2,
      HashingMapper.MD5,
      HashingMapper.SHA1,
      HashingMapper.SHA224,
      HashingMapper.SHA256,
      HashingMapper.SHA384,
      HashingMapper.SHA512,
    ],
  )
  fun mapHashesFieldCorrectly(hashingMethod: String) {
    val config =
      HashingMapperConfig(
        MAPPER_NAME,
        null,
        HashingConfig(
          "field1",
          HashingMethods.fromValue(hashingMethod)!!,
          "_hashed",
        ),
      )

    val record = TestRecordAdapter(StreamDescriptor().withName("stream"), mutableMapOf("field1" to "value1", "field2" to "value2"))
    hashingMapper.map(config, record)

    assertTrue(record.has("field1_hashed"))
    val hashedValue = record.get("field1_hashed").asString()
    assertTrue("hashed_value" != hashedValue && hashedValue.all { it.isLetterOrDigit() }, "$hashedValue doesn't look like a valid hash")
    assertFalse(record.has("field1"))
    assertTrue(record.has("field2"))
    assertEquals("value2", record.get("field2").asString())
  }

  @Test
  fun testAllMethodAreSupported() {
    val messageDigestAlgorithms = Security.getAlgorithms("MessageDigest")
    assertTrue(messageDigestAlgorithms.containsAll(supportedMethods))
  }

  @Nested
  internal inner class HashingMapperSpecTest {
    @Test
    fun specReturnsCorrectSpecification() {
      val spec = hashingMapper.spec().jsonSchema()
      assertEquals(
        hashingMapper.name,
        spec
          .get("properties")
          .get("name")
          .get("const")
          .asText()!!,
      )
      assertTrue(spec.get("properties").contains("documentationUrl"))
      assertTrue(
        spec
          .get("properties")
          .get("config")
          .get("properties")
          .contains(HashingMapper.TARGET_FIELD_CONFIG_KEY),
      )
      assertTrue(
        spec
          .get("properties")
          .get("config")
          .get("properties")
          .contains(HashingMapper.METHOD_CONFIG_KEY),
      )
      assertTrue(
        spec
          .get("properties")
          .get("config")
          .get("properties")
          .contains(HashingMapper.FIELD_NAME_SUFFIX_CONFIG_KEY),
      )
    }

    // If making changes to the mapper spec, ensure this test passes without modifying the examples to guarantee backward compatibility.
    @Test
    fun configExamplesShouldMapToSpec() {
      val resource =
        javaClass.classLoader.getResource("HashingMapperConfigExamples.json")
          ?: throw IllegalArgumentException("File not found: HashingMapperConfigExamples.json")

      val configExamples = TEST_OBJECT_MAPPER.readValue(File(resource.toURI()), object : TypeReference<List<ConfiguredMapper>>() {})

      configExamples.forEachIndexed { index, configExample ->
        try {
          hashingMapper.spec().deserialize(configExample)
        } catch (e: IllegalArgumentException) {
          throw AssertionError("Example at index $index is not valid: ${e.message}", e)
        }
      }
    }

    @Test
    fun mapHandlesUnsupportedHashingMethod() {
      val config =
        ConfiguredMapper(
          MAPPER_NAME,
          TEST_OBJECT_MAPPER.valueToTree(
            mapOf(
              HashingMapper.TARGET_FIELD_CONFIG_KEY to "field1",
              HashingMapper.METHOD_CONFIG_KEY to "unsupported",
              HashingMapper.FIELD_NAME_SUFFIX_CONFIG_KEY to "_hashed",
            ),
          ),
        )

      val exception =
        assertThrows(IllegalArgumentException::class.java) {
          hashingMapper.spec().deserialize(config)
        }

      assertEquals(
        "Mapper Config not valid: \$.config.method: does not have a value in the " +
          "enumeration [\"MD2\", \"MD5\", \"SHA-1\", \"SHA-224\", \"SHA-256\", \"SHA-384\", \"SHA-512\"]",
        exception.message,
      )
    }

    @Test
    fun schemaTransformsFailsIfFieldIsMissing() {
      val config =
        ConfiguredMapper(
          MAPPER_NAME,
          TEST_OBJECT_MAPPER.valueToTree(
            mapOf(
              HashingMapper.TARGET_FIELD_CONFIG_KEY to "field1",
              HashingMapper.FIELD_NAME_SUFFIX_CONFIG_KEY to "_hashed",
            ),
          ),
        )
      val exception =
        assertThrows(IllegalArgumentException::class.java) {
          hashingMapper.spec().deserialize(config)
        }
      assertEquals(
        "Mapper Config not valid: \$.config: required property 'method' not found",
        exception.message,
      )
    }

    @Test
    fun schemaTransformFailsIfFieldCollision() {
      val config =
        ConfiguredMapper(
          MAPPER_NAME,
          TEST_OBJECT_MAPPER.valueToTree(
            mapOf(
              HashingMapper.TARGET_FIELD_CONFIG_KEY to "field1",
              HashingMapper.FIELD_NAME_SUFFIX_CONFIG_KEY to "_hashed",
            ),
          ),
        )
      val exception =
        assertThrows(IllegalArgumentException::class.java) {
          hashingMapper.spec().deserialize(config)
        }
      assertEquals(
        "Mapper Config not valid: \$.config: required property 'method' not found",
        exception.message,
      )
    }
  }
}
