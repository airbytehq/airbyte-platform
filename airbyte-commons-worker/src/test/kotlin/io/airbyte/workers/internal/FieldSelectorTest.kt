/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal

import io.airbyte.commons.json.Jsons
import io.airbyte.config.AirbyteStream
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.SyncMode
import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.protocol.models.AirbyteRecordMessage
import io.airbyte.workers.RecordSchemaValidator
import io.airbyte.workers.WorkerUtils
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

internal class FieldSelectorTest {
  companion object {
    private const val STREAM_NAME = "name"

    private val SCHEMA =
      """
      {
        "type": ["null", "object"],
        "properties": {
          "key": {"type": ["null", "string"]},
          "value": {"type": ["null", "string"]}
        }
      }
      """.trimIndent()

    private const val ESCAPED_ID = "\$id"
    private val SCHEMA_WITH_ESCAPE =
      """
      {
        "type": ["null", "object"],
        "properties": {
          "$ESCAPED_ID": {"type": ["null", "string"]},
          "key": {"type": ["null", "string"]},
          "value": {"type": ["null", "string"]}
        }
      }
      """.trimIndent()

    private val SCHEMA_WITH_DOLLAR_SIGNS =
      """
      {
        "type": ["null", "object"],
        "properties": {
          "test${'$'}ign": {"type": ["null", "string"]},
          "test${'$'}id": {"type": ["null", "string"]},
          "test${'$'}schema": {"type": ["null", "string"]},
          "test${'$'}comment": {"type": ["null", "string"]},
          "key": {"type": ["null", "string"]},
          "value": {"type": ["null", "string"]}
        }
      }
      """.trimIndent()

    private val RECORD_WITH_DOLLAR_SIGNS =
      """
      {
        "test${'$'}ign": "myId",
        "test${'$'}id": "id field",
        "test${'$'}schema": "schema field",
        "test${'$'}comment": "comment field",
        "key": "myKey",
        "value": "myValue",
        "unexpected": "strip me"
      }
      """.trimIndent()

    private val RECORD_WITH_DOLLAR_SIGNS_WITHOUT_EXTRA =
      """
      {
        "test${'$'}ign": "myId",
        "test${'$'}id": "id field",
        "test${'$'}schema": "schema field",
        "test${'$'}comment": "comment field",
        "key": "myKey",
        "value": "myValue"
      }
      """.trimIndent()

    private val RECORD_WITH_EXTRA =
      """
      {
        "id": "myId",
        "key": "myKey",
        "value": "myValue",
        "unexpected": "strip me"
      }
      """.trimIndent()

    private val RECORD_WITHOUT_EXTRA =
      """
      {
        "key": "myKey",
        "value": "myValue"
      }
      """.trimIndent()

    private val RECORD_WITH_ID_WITHOUT_EXTRA =
      """
      {
        "id": "myId",
        "key": "myKey",
        "value": "myValue"
      }
      """.trimIndent()
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  internal fun `test that we filter columns`(fieldSelectionEnabled: Boolean) {
    val configuredCatalog =
      ConfiguredAirbyteCatalog()
        .withStreams(
          listOf(
            ConfiguredAirbyteStream(
              stream = AirbyteStream(STREAM_NAME, Jsons.deserialize(SCHEMA), listOf(SyncMode.INCREMENTAL)),
              syncMode = SyncMode.INCREMENTAL,
              destinationSyncMode = DestinationSyncMode.APPEND,
            ),
          ),
        )

    val fieldSelector = createFieldSelector(configuredCatalog, fieldSelectionEnabled = fieldSelectionEnabled)

    val message = createRecord(RECORD_WITH_EXTRA)
    fieldSelector.filterSelectedFields(message)

    val expectedMessage = if (fieldSelectionEnabled) createRecord(RECORD_WITHOUT_EXTRA) else createRecord(RECORD_WITH_EXTRA)
    assertEquals(expectedMessage, message)
  }

  @Test
  internal fun `test that escaped properties in schema are still filtered`() {
    val configuredCatalog =
      ConfiguredAirbyteCatalog()
        .withStreams(
          listOf(
            ConfiguredAirbyteStream(
              stream =
                AirbyteStream(
                  name = STREAM_NAME,
                  jsonSchema = Jsons.deserialize(SCHEMA_WITH_ESCAPE),
                  supportedSyncModes = listOf(SyncMode.INCREMENTAL),
                ),
              syncMode = SyncMode.INCREMENTAL,
              destinationSyncMode = DestinationSyncMode.APPEND,
            ),
          ),
        )

    val fieldSelector = createFieldSelector(configuredCatalog, fieldSelectionEnabled = true)

    val message = createRecord(RECORD_WITH_EXTRA)
    fieldSelector.filterSelectedFields(message)

    val expectedMessage = createRecord(RECORD_WITH_ID_WITHOUT_EXTRA)
    assertEquals(expectedMessage, message)
  }

  @Test
  internal fun `test we select columns with dollar signs`() {
    val configuredCatalog =
      ConfiguredAirbyteCatalog()
        .withStreams(
          listOf(
            ConfiguredAirbyteStream(
              stream =
                AirbyteStream(
                  name = STREAM_NAME,
                  jsonSchema = Jsons.deserialize(SCHEMA_WITH_DOLLAR_SIGNS),
                  supportedSyncModes = listOf(SyncMode.INCREMENTAL),
                ),
              syncMode = SyncMode.INCREMENTAL,
              destinationSyncMode = DestinationSyncMode.APPEND,
            ),
          ),
        )

    val fieldSelector = createFieldSelector(configuredCatalog, fieldSelectionEnabled = true)

    val message = createRecord(RECORD_WITH_DOLLAR_SIGNS)
    fieldSelector.filterSelectedFields(message)

    val expectedMessage = createRecord(RECORD_WITH_DOLLAR_SIGNS_WITHOUT_EXTRA)
    assertEquals(expectedMessage, message)
  }

  private fun createFieldSelector(
    configuredCatalog: ConfiguredAirbyteCatalog,
    fieldSelectionEnabled: Boolean,
  ): FieldSelector {
    val schemaValidator = RecordSchemaValidator(WorkerUtils.mapStreamNamesToSchemas(configuredCatalog))
    val fieldSelector =
      FieldSelector(
        schemaValidator,
        mockk(),
        fieldSelectionEnabled,
        false,
      )
    fieldSelector.populateFields(configuredCatalog)
    return fieldSelector
  }

  private fun createRecord(jsonData: String): AirbyteMessage =
    AirbyteMessage()
      .withType(AirbyteMessage.Type.RECORD)
      .withRecord(
        AirbyteRecordMessage()
          .withStream(STREAM_NAME)
          .withData(Jsons.deserialize(jsonData)),
      )
}
