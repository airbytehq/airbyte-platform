package io.airbyte.mappers.transformations

import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.Field
import io.airbyte.config.FieldType
import io.airbyte.mappers.transformations.HashingMapper.Companion.supportedMethods
import io.mockk.every
import io.mockk.spyk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.security.Security

class HashingMapperTest {
  private val hashingMapper = spyk(HashingMapper())

  private val testObject =
    Jsons.deserialize(
      """
    {
      "field1": "value1",
      "field2": "value2"
    }
  """,
    ) as ObjectNode

  @Test
  fun specReturnsCorrectSpecification() {
    val spec = hashingMapper.spec()
    assertEquals("hashing", spec.name)
    assertEquals("", spec.documentationUrl)
    assertTrue(spec.config.containsKey(HashingMapper.TARGET_FIELD_CONFIG_KEY))
    assertTrue(spec.config.containsKey(HashingMapper.METHOD_CONFIG_KEY))
    assertTrue(spec.config.containsKey(HashingMapper.FIELD_NAME_SUFFIX_CONFIG_KEY))
  }

  @Test
  fun schemaTransformsFieldNamesCorrectly() {
    val config =
      ConfiguredMapper("test", mapOf(HashingMapper.TARGET_FIELD_CONFIG_KEY to "field1", HashingMapper.FIELD_NAME_SUFFIX_CONFIG_KEY to "_hashed"))
    val fields = listOf(Field("field1", FieldType.DATE), Field("field2", FieldType.DATE))
    val result = hashingMapper.schema(config, fields)
    assertEquals(listOf(Field("field1_hashed", FieldType.STRING), Field("field2", FieldType.DATE)), result)
  }

  @Test
  fun schemaTransformsFieldNamesCorrectlyIfMissingSuffix() {
    val config =
      ConfiguredMapper("test", mapOf(HashingMapper.TARGET_FIELD_CONFIG_KEY to "field1"))
    val fields = listOf(Field("field1", FieldType.DATE), Field("field2", FieldType.DATE))
    val result = hashingMapper.schema(config, fields)
    assertEquals(listOf(Field("field1_hashed", FieldType.STRING), Field("field2", FieldType.DATE)), result)
  }

  @Test
  fun schemaTransformsFailsIfFieldIsMissing() {
    val config =
      ConfiguredMapper("test", mapOf(HashingMapper.TARGET_FIELD_CONFIG_KEY to "field1", HashingMapper.FIELD_NAME_SUFFIX_CONFIG_KEY to "_hashed"))
    val fields = listOf(Field("NotField1", FieldType.STRING))
    assertThrows(IllegalStateException::class.java) {
      hashingMapper.schema(config, fields)
    }
  }

  @Test
  fun schemaTransformFailsIfFieldCollision() {
    val config =
      ConfiguredMapper("test", mapOf(HashingMapper.TARGET_FIELD_CONFIG_KEY to "field1", HashingMapper.FIELD_NAME_SUFFIX_CONFIG_KEY to "_hashed"))
    val fields = listOf(Field("field1", FieldType.STRING), Field("field1_hashed", FieldType.STRING))
    assertThrows(IllegalStateException::class.java) {
      hashingMapper.schema(config, fields)
    }
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
      ConfiguredMapper(
        "test",
        mapOf(
          HashingMapper.TARGET_FIELD_CONFIG_KEY to "field1",
          HashingMapper.METHOD_CONFIG_KEY to HashingMapper.SHA256,
          HashingMapper.FIELD_NAME_SUFFIX_CONFIG_KEY to "_hashed",
        ),
      )

    every { hashingMapper.hashAndEncodeData(HashingMapper.SHA256, "value1".toByteArray()) } returns "hashed_value"

    val record = Record(testObject)
    hashingMapper.map(config, record)

    assertTrue(record.data.has("field1_hashed"))
    assertEquals("hashed_value", record.data.get("field1_hashed").asText())
    assertFalse(record.data.has("field1"))
    assertTrue(record.data.has("field2"))
    assertEquals("value2", record.data.get("field2").asText())
  }

  @Test
  fun mapHandlesUnsupportedHashingMethod() {
    val config =
      ConfiguredMapper(
        "test",
        mapOf(
          HashingMapper.TARGET_FIELD_CONFIG_KEY to "field1",
          HashingMapper.METHOD_CONFIG_KEY to "unsupported",
          HashingMapper.FIELD_NAME_SUFFIX_CONFIG_KEY to "_hashed",
        ),
      )
    val record = Record(testObject)
    assertThrows(IllegalArgumentException::class.java) {
      hashingMapper.map(config, record)
    }
  }

  @Test
  fun testAllMethodAreSupported() {
    val messageDigestAlgorithms = Security.getAlgorithms("MessageDigest")
    assertTrue(messageDigestAlgorithms.containsAll(supportedMethods))
  }
}
