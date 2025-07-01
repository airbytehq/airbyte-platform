/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.model.adapter

import io.airbyte.commons.json.Jsons
import io.airbyte.config.StreamDescriptor
import io.airbyte.mappers.adapters.AirbyteRecord
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteRecordMessageMetaChange
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class JsonRecordAdapterTest {
  companion object {
    const val BOOLEAN_FIELD = "boolean-field"
    const val INT_FIELD = "int-field"
    const val NUMBER_FIELD = "number-field"
    const val STRING_FIELD = "string-field"
  }

  private val jsonRecordString =
    """
    {
      "type": "RECORD",
      "record": {
        "stream": "stream-name",
        "namespace": "stream-namespace",
        "emitted_at": 1337,
        "data": {
          "$STRING_FIELD": "bar",
          "$BOOLEAN_FIELD": true,
          "$INT_FIELD": 42,
          "$NUMBER_FIELD": 4.2
        }
      }
    }
    """.trimIndent()

  @Test
  fun `basic read`() {
    val adapter = getAdapterFromRecord(jsonRecordString)

    Assertions.assertEquals(
      StreamDescriptor().withName("stream-name").withNamespace("stream-namespace"),
      adapter.streamDescriptor,
    )

    Assertions.assertEquals("bar", adapter.get(STRING_FIELD).asString())
    Assertions.assertEquals(false, adapter.get(STRING_FIELD).asBoolean())

    Assertions.assertEquals(true, adapter.get(BOOLEAN_FIELD).asBoolean())
    Assertions.assertEquals("true", adapter.get(BOOLEAN_FIELD).asString())

    Assertions.assertEquals("42", adapter.get(INT_FIELD).asString())

    Assertions.assertEquals("4.2", adapter.get(NUMBER_FIELD).asString())
  }

  @Test
  fun `renaming a field creates the new field and removes the old one`() {
    val adapter = getAdapterFromRecord(jsonRecordString)
    val newFieldName = "$STRING_FIELD-new"

    Assertions.assertFalse(adapter.has(newFieldName))

    adapter.rename(STRING_FIELD, newFieldName)

    Assertions.assertEquals("bar", adapter.get(newFieldName).asString())
    Assertions.assertFalse(adapter.has(STRING_FIELD))
  }

  @Test
  fun `renaming to the same name does nothing`() {
    val adapter = getAdapterFromRecord(jsonRecordString)
    adapter.rename(STRING_FIELD, STRING_FIELD)
    Assertions.assertEquals("bar", adapter.get(STRING_FIELD).asString())
  }

  @Test
  fun `tracking meta changes`() {
    val adapter = getAdapterFromRecord(jsonRecordString)

    adapter.trackFieldError("test", AirbyteRecord.Change.TRUNCATED, AirbyteRecord.Reason.PLATFORM_SERIALIZATION_ERROR)

    val expectedMetaChange =
      AirbyteRecordMessageMetaChange()
        .withField("test")
        .withChange(AirbyteRecordMessageMetaChange.Change.TRUNCATED)
        .withReason(AirbyteRecordMessageMetaChange.Reason.PLATFORM_SERIALIZATION_ERROR)
    Assertions.assertEquals(listOf(expectedMetaChange), adapter.asProtocol.record.meta.changes)
  }

  @Test
  fun `verify modify then serialize`() {
    val adapter = getAdapterFromRecord(jsonRecordString)
    adapter.set(STRING_FIELD, "woohoo")

    val serialized = Jsons.serialize(adapter.asProtocol)
    val deserialized = Jsons.deserialize(serialized, AirbyteMessage::class.java)
    Assertions.assertEquals(adapter.asProtocol, deserialized)
  }

  @Test
  fun `writing boolean`() {
    val adapter = getAdapterFromRecord(jsonRecordString)

    adapter.set(STRING_FIELD, true)
    Assertions.assertEquals(true, adapter.get(STRING_FIELD).asBoolean())

    adapter.set(BOOLEAN_FIELD, false)
    Assertions.assertEquals(false, adapter.get(BOOLEAN_FIELD).asBoolean())
  }

  @Test
  fun `writing double`() {
    val adapter = getAdapterFromRecord(jsonRecordString)

    adapter.set(NUMBER_FIELD, 1.1)
    Assertions.assertEquals(1.1, adapter.get(NUMBER_FIELD).asNumber())

    adapter.set(STRING_FIELD, 2)
    Assertions.assertEquals(2.0, adapter.get(STRING_FIELD).asNumber())
  }

  @Test
  fun `writing numbers`() {
    val adapter = getAdapterFromRecord(jsonRecordString)

    adapter.set(NUMBER_FIELD, 1)
    Assertions.assertEquals(1.0, adapter.get(NUMBER_FIELD).asNumber())

    adapter.set(STRING_FIELD, 2)
    Assertions.assertEquals(2.0, adapter.get(STRING_FIELD).asNumber())
  }

  @Test
  fun `writing strings`() {
    val adapter = getAdapterFromRecord(jsonRecordString)

    adapter.set(STRING_FIELD, "updated")
    Assertions.assertEquals("updated", adapter.get(STRING_FIELD).asString())

    adapter.set(BOOLEAN_FIELD, "overridden")
    Assertions.assertEquals("overridden", adapter.get(BOOLEAN_FIELD).asString())
  }

  fun getAdapterFromRecord(jsonString: String) = AirbyteJsonRecordAdapter(getRecord(jsonString))

  fun getRecord(jsonString: String): AirbyteMessage = Jsons.deserialize(jsonString, AirbyteMessage::class.java)
}
