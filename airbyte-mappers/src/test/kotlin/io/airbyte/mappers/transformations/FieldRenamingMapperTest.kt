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
import io.airbyte.config.mapper.configs.FieldRenamingConfig
import io.airbyte.config.mapper.configs.FieldRenamingMapperConfig
import io.airbyte.mappers.adapters.AirbyteRecord
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.io.File

@MicronautTest
class FieldRenamingMapperTest {
  private val mapperType = "field-renaming"

  @Inject
  lateinit var mapper: FieldRenamingMapper

  private fun getRecord(recordData: MutableMap<String, Any>): AirbyteRecord =
    TestRecordAdapter(
      streamDescriptor = StreamDescriptor(),
      data = recordData,
    )

  private fun createConfiguredMapper(config: FieldRenamingConfig): FieldRenamingMapperConfig = FieldRenamingMapperConfig(mapperType, null, config)

  @Test
  fun `should rename field when original field exists`() {
    val config = FieldRenamingConfig("oldField", "newField")
    val configuredMapper = createConfiguredMapper(config)
    val recordData =
      mutableMapOf<String, Any>(
        "oldField" to 1,
        "otherField" to "otherValue",
      )
    val record = getRecord(recordData)

    mapper.map(configuredMapper, record)

    assertFalse(record.has("oldField"), "Old field should be removed")
    assertTrue(record.has("newField"), "New field should be present")
    assertEquals("1", record.get("newField").asString())
    assertEquals("otherValue", record.get("otherField").asString())
  }

  @Test
  fun `should do nothing when original field does not exist`() {
    val config =
      FieldRenamingConfig(
        "nonExistentField",
        "newField",
      )
    val configuredMapper = createConfiguredMapper(config)
    val recordData =
      mutableMapOf<String, Any>(
        "someField" to "value",
      )
    val record = getRecord(recordData)

    mapper.map(configuredMapper, record)

    assertFalse(record.has("nonExistentField"), "Non-existent field should not be present")
    assertFalse(record.has("newField"), "New field should not be created")
    assertTrue(record.has("someField"), "Existing fields should remain untouched")
    assertEquals("value", record.get("someField").asString())
  }

  @Test
  fun `should update schema correctly after renaming`() {
    val config =
      FieldRenamingConfig(
        "oldField",
        "newField",
      )
    val configuredMapper = createConfiguredMapper(config)
    val slimStream = SlimStream(fields = listOf(Field("oldField", FieldType.STRING), Field("otherField", FieldType.STRING)))

    val updatedSlimStream = mapper.schema(configuredMapper, slimStream)
    val fieldNames = updatedSlimStream.fields.map { it.name }

    assertFalse("oldField" in fieldNames, "Old field should be removed from schema")
    assertTrue("newField" in fieldNames, "New field should be added to schema")
    assertTrue("otherField" in fieldNames, "Other fields should remain in schema")
  }

  @Nested
  internal inner class FieldRenamingMapperSpecTest {
    @Test
    fun `should return expected spec`() {
      val spec = mapper.spec().jsonSchema()
      assertEquals(
        mapper.name,
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
          .contains(FieldRenamingMapper.ORIGINAL_FIELD_NAME),
      )
      assertTrue(
        spec
          .get("properties")
          .get("config")
          .get("properties")
          .contains(FieldRenamingMapper.NEW_FIELD_NAME),
      )
    }

    // If making changes to the mapper spec, ensure this test passes without modifying the examples to guarantee backward compatibility.
    @Test
    fun `should successfully validate config examples against spec`() {
      val resource =
        javaClass.classLoader.getResource("FieldRenamingMapperConfigExamples.json")
          ?: throw IllegalArgumentException("File not found: FieldRenamingMapperConfigExamples.json")

      val configExamples = TEST_OBJECT_MAPPER.readValue(File(resource.toURI()), object : TypeReference<List<ConfiguredMapper>>() {})

      configExamples.forEachIndexed { index, configExample ->
        try {
          mapper.spec().deserialize(configExample)
        } catch (e: IllegalArgumentException) {
          throw AssertionError("Example at index $index is not valid: ${e.message}", e)
        }
      }
    }

    @Test
    fun `should throw exception when config is missing original field name`() {
      val configuredMapper = ConfiguredMapper(mapperType, TEST_OBJECT_MAPPER.valueToTree(mapOf("newFieldName" to "newField")))
      val exception =
        assertThrows(IllegalArgumentException::class.java) {
          mapper.spec().deserialize(configuredMapper)
        }

      assertEquals("Mapper Config not valid: \$.config: required property 'originalFieldName' not found", exception.message)
    }

    @Test
    fun `should throw exception when config is missing new field name`() {
      val configuredMapper = ConfiguredMapper(mapperType, TEST_OBJECT_MAPPER.valueToTree(mapOf("originalFieldName" to "oldField")))
      val exception =
        assertThrows(IllegalArgumentException::class.java) {
          mapper.spec().deserialize(configuredMapper)
        }

      assertEquals("Mapper Config not valid: \$.config: required property 'newFieldName' not found", exception.message)
    }
  }
}
