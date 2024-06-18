package io.airbyte.workers.internal

import io.airbyte.commons.json.Jsons
import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.protocol.models.AirbyteRecordMessage
import io.airbyte.protocol.models.AirbyteStream
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog
import io.airbyte.protocol.models.ConfiguredAirbyteStream
import io.airbyte.workers.RecordSchemaValidator
import io.airbyte.workers.WorkerUtils
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class FieldSelectorTest {
  companion object {
    private val STREAM_NAME = "name"

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
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun `test that we filter columns`(fieldSelectionEnabled: Boolean) {
    val configuredCatalog =
      ConfiguredAirbyteCatalog()
        .withStreams(
          listOf(
            ConfiguredAirbyteStream()
              .withStream(
                AirbyteStream().withName(STREAM_NAME).withJsonSchema(Jsons.deserialize(SCHEMA)),
              ),
          ),
        )

    val fieldSelector = createFieldSelector(configuredCatalog, fieldSelectionEnabled = fieldSelectionEnabled)

    val message = createRecord(RECORD_WITH_EXTRA)
    fieldSelector.filterSelectedFields(message)

    val expectedMessage = if (fieldSelectionEnabled) createRecord(RECORD_WITHOUT_EXTRA) else createRecord(RECORD_WITH_EXTRA)
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
