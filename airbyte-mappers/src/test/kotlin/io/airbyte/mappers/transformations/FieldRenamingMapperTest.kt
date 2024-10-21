package io.airbyte.mappers.transformations

import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.Field
import io.airbyte.config.FieldType
import io.airbyte.config.adapters.AirbyteJsonRecordAdapter
import io.airbyte.config.adapters.AirbyteRecord
import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.protocol.models.AirbyteRecordMessage
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Instant

class FieldRenamingMapperTest {
  private val mapperType = "FIELD_RENAMING_MAPPER"
  private val mapper = FieldRenamingMapper()

  private fun getRecord(recordData: Map<String, Any>): AirbyteRecord {
    return AirbyteJsonRecordAdapter(
      AirbyteMessage()
        .withType(AirbyteMessage.Type.RECORD)
        .withRecord(
          AirbyteRecordMessage()
            .withNamespace("NAMESPACE")
            .withStream("STREAM")
            .withEmittedAt(Instant.now().toEpochMilli())
            .withData(Jsons.jsonNode(recordData)),
        ),
    )
  }

  private fun createConfiguredMapper(config: Map<String, String>): ConfiguredMapper {
    return ConfiguredMapper(mapperType, config)
  }

  @Test
  fun `should rename field when original field exists`() {
    val config =
      mapOf(
        "originalFieldName" to "oldField",
        "newFieldName" to "newField",
      )
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
      mapOf(
        "originalFieldName" to "nonExistentField",
        "newFieldName" to "newField",
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
  fun `should throw exception when config is missing original field name`() {
    val config =
      mapOf(
        "newFieldName" to "newField",
      )
    val configuredMapper = createConfiguredMapper(config)
    val recordData =
      mutableMapOf<String, Any>(
        "oldField" to "value",
      )
    val record = getRecord(recordData)

    val exception =
      assertThrows(IllegalArgumentException::class.java) {
        mapper.map(configuredMapper, record)
      }

    assertEquals("Config missing required key: originalFieldName", exception.message)
  }

  @Test
  fun `should throw exception when config is missing new field name`() {
    val config =
      mapOf(
        "originalFieldName" to "oldField",
      )
    val configuredMapper = createConfiguredMapper(config)
    val recordData =
      mutableMapOf<String, Any>(
        "oldField" to "value",
      )
    val record = getRecord(recordData)

    val exception =
      assertThrows(IllegalArgumentException::class.java) {
        mapper.map(configuredMapper, record)
      }

    assertEquals("Config missing required key: newFieldName", exception.message)
  }

  @Test
  fun `should update schema correctly after renaming`() {
    val config =
      mapOf(
        "originalFieldName" to "oldField",
        "newFieldName" to "newField",
      )
    val configuredMapper = createConfiguredMapper(config)
    val slimStream = SlimStream(fields = listOf(Field("oldField", FieldType.STRING), Field("otherField", FieldType.STRING)))

    val updatedSlimStream = mapper.schema(configuredMapper, slimStream)
    val fieldNames = updatedSlimStream.fields.map { it.name }

    assertFalse("oldField" in fieldNames, "Old field should be removed from schema")
    assertTrue("newField" in fieldNames, "New field should be added to schema")
    assertTrue("otherField" in fieldNames, "Other fields should remain in schema")
  }
}
