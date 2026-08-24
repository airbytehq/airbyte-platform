/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.filter

import io.airbyte.commons.json.Jsons
import io.airbyte.config.AirbyteStream
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.SyncMode
import io.airbyte.container.orchestrator.worker.RecordSchemaValidator
import io.airbyte.container.orchestrator.worker.context.ReplicationInputFeatureFlagReader
import io.airbyte.container.orchestrator.worker.util.ReplicationMetricReporter
import io.airbyte.featureflag.RemoveValidationLimit
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair
import io.airbyte.validation.json.JsonSchemaValidator
import io.airbyte.workers.WorkerUtils
import io.airbyte.workers.models.DeclaredStreamFields
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.Executors

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

  @Test
  internal fun `test that we filter columns`() {
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
    val replicationInput =
      mockk<ReplicationInput> {
        every { workspaceId } returns UUID.randomUUID()
      }

    val fieldSelector =
      createFieldSelector(configuredCatalog = configuredCatalog, replicationInput = replicationInput)

    val message = createRecord(RECORD_WITH_EXTRA)
    fieldSelector.filterSelectedFields(message)

    Assertions.assertEquals(createRecord(RECORD_WITHOUT_EXTRA), message)
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
    val replicationInput =
      mockk<ReplicationInput> {
        every { workspaceId } returns UUID.randomUUID()
      }

    val fieldSelector = createFieldSelector(configuredCatalog = configuredCatalog, replicationInput = replicationInput)

    val message = createRecord(RECORD_WITH_EXTRA)
    fieldSelector.filterSelectedFields(message)

    val expectedMessage = createRecord(RECORD_WITH_ID_WITHOUT_EXTRA)
    Assertions.assertEquals(expectedMessage, message)
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
    val replicationInput =
      mockk<ReplicationInput> {
        every { workspaceId } returns UUID.randomUUID()
      }

    val fieldSelector = createFieldSelector(configuredCatalog = configuredCatalog, replicationInput = replicationInput)

    val message = createRecord(RECORD_WITH_DOLLAR_SIGNS)
    fieldSelector.filterSelectedFields(message)

    val expectedMessage = createRecord(RECORD_WITH_DOLLAR_SIGNS_WITHOUT_EXTRA)
    Assertions.assertEquals(expectedMessage, message)
  }

  @Test
  internal fun `test that unexpected fields are tracked before filtering`() {
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
    val replicationInput =
      mockk<ReplicationInput> {
        every { workspaceId } returns UUID.randomUUID()
      }
    val metricReporter = mockk<ReplicationMetricReporter>(relaxed = true)

    val fieldSelector =
      createFieldSelector(configuredCatalog = configuredCatalog, replicationInput = replicationInput, metricReporter = metricReporter)

    val message = createRecord(RECORD_WITH_EXTRA)
    fieldSelector.trackUnexpectedFields(message)
    fieldSelector.filterSelectedFields(message)
    fieldSelector.validateSchema(message)

    Assertions.assertEquals(createRecord(RECORD_WITHOUT_EXTRA), message)
    fieldSelector.reportMetrics(UUID.randomUUID())
    verify(exactly = 1) {
      metricReporter.trackUnexpectedFields(AirbyteStreamNameNamespacePair(STREAM_NAME, null), mutableSetOf("id", "unexpected"))
    }
  }

  @Test
  internal fun `test declared fields include deselected fields for unexpected field tracking`() {
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
    val replicationInput =
      mockk<ReplicationInput> {
        every { workspaceId } returns UUID.randomUUID()
      }
    val metricReporter = mockk<ReplicationMetricReporter>(relaxed = true)
    val declaredFields =
      listOf(
        DeclaredStreamFields(
          StreamDescriptor().withName(STREAM_NAME),
          listOf("key", "value", "id"),
        ),
      )

    val fieldSelector =
      createFieldSelector(
        configuredCatalog = configuredCatalog,
        replicationInput = replicationInput,
        metricReporter = metricReporter,
        declaredStreamFields = declaredFields,
      )

    val message = createRecord(RECORD_WITH_EXTRA)
    fieldSelector.trackUnexpectedFields(message)
    fieldSelector.filterSelectedFields(message)
    fieldSelector.validateSchema(message)

    Assertions.assertEquals(createRecord(RECORD_WITHOUT_EXTRA), message)
    fieldSelector.reportMetrics(UUID.randomUUID())
    verify(exactly = 1) {
      metricReporter.trackUnexpectedFields(AirbyteStreamNameNamespacePair(STREAM_NAME, null), mutableSetOf("unexpected"))
    }
  }

  @Test
  internal fun `test empty declared fields fall back to configured catalog`() {
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
    val replicationInput =
      mockk<ReplicationInput> {
        every { workspaceId } returns UUID.randomUUID()
      }
    val metricReporter = mockk<ReplicationMetricReporter>(relaxed = true)
    val fieldSelector =
      createFieldSelector(
        configuredCatalog = configuredCatalog,
        replicationInput = replicationInput,
        metricReporter = metricReporter,
        declaredStreamFields = emptyList(),
      )

    val message = createRecord(RECORD_WITH_EXTRA)
    fieldSelector.trackUnexpectedFields(message)
    fieldSelector.filterSelectedFields(message)
    fieldSelector.validateSchema(message)

    Assertions.assertEquals(createRecord(RECORD_WITHOUT_EXTRA), message)
    fieldSelector.reportMetrics(UUID.randomUUID())
    verify(exactly = 1) {
      metricReporter.trackUnexpectedFields(AirbyteStreamNameNamespacePair(STREAM_NAME, null), mutableSetOf("id", "unexpected"))
    }
  }

  @Test
  internal fun `test declared fields fall back to configured catalog per stream`() {
    val fallbackStreamName = "fallback"
    val configuredCatalog =
      ConfiguredAirbyteCatalog()
        .withStreams(
          listOf(
            ConfiguredAirbyteStream(
              stream = AirbyteStream(STREAM_NAME, Jsons.deserialize(SCHEMA), listOf(SyncMode.INCREMENTAL)),
              syncMode = SyncMode.INCREMENTAL,
              destinationSyncMode = DestinationSyncMode.APPEND,
            ),
            ConfiguredAirbyteStream(
              stream = AirbyteStream(fallbackStreamName, Jsons.deserialize(SCHEMA), listOf(SyncMode.INCREMENTAL)),
              syncMode = SyncMode.INCREMENTAL,
              destinationSyncMode = DestinationSyncMode.APPEND,
            ),
          ),
        )
    val replicationInput =
      mockk<ReplicationInput> {
        every { workspaceId } returns UUID.randomUUID()
      }
    val metricReporter = mockk<ReplicationMetricReporter>(relaxed = true)
    val fieldSelector =
      createFieldSelector(
        configuredCatalog = configuredCatalog,
        replicationInput = replicationInput,
        metricReporter = metricReporter,
        declaredStreamFields =
          listOf(
            DeclaredStreamFields(
              StreamDescriptor().withName(STREAM_NAME),
              listOf("key", "value", "id"),
            ),
          ),
      )

    val declaredStreamMessage = createRecord(STREAM_NAME, RECORD_WITH_EXTRA)
    fieldSelector.trackUnexpectedFields(declaredStreamMessage)
    fieldSelector.filterSelectedFields(declaredStreamMessage)
    val fallbackStreamMessage = createRecord(fallbackStreamName, RECORD_WITH_EXTRA)
    fieldSelector.trackUnexpectedFields(fallbackStreamMessage)
    fieldSelector.filterSelectedFields(fallbackStreamMessage)

    Assertions.assertEquals(createRecord(STREAM_NAME, RECORD_WITHOUT_EXTRA), declaredStreamMessage)
    Assertions.assertEquals(createRecord(fallbackStreamName, RECORD_WITHOUT_EXTRA), fallbackStreamMessage)
    fieldSelector.reportMetrics(UUID.randomUUID())
    verify(exactly = 1) {
      metricReporter.trackUnexpectedFields(AirbyteStreamNameNamespacePair(STREAM_NAME, null), mutableSetOf("unexpected"))
      metricReporter.trackUnexpectedFields(AirbyteStreamNameNamespacePair(fallbackStreamName, null), mutableSetOf("id", "unexpected"))
    }
  }

  private fun createFieldSelector(
    configuredCatalog: ConfiguredAirbyteCatalog,
    replicationInput: ReplicationInput,
    metricReporter: ReplicationMetricReporter = mockk(),
    declaredStreamFields: List<DeclaredStreamFields>? = null,
  ): FieldSelector {
    every { replicationInput.declaredStreamFields } returns declaredStreamFields
    val replicationInputFeatureFlagReader =
      mockk<ReplicationInputFeatureFlagReader> {
        every { read(RemoveValidationLimit) } returns false
      }
    val schemaValidator =
      RecordSchemaValidator(
        jsonSchemaValidator = JsonSchemaValidator(),
        schemaValidationExecutorService = Executors.newSingleThreadExecutor(),
        streamNamesToSchemas = WorkerUtils.mapStreamNamesToSchemas(configuredCatalog),
      )
    val fieldSelector =
      FieldSelector(
        recordSchemaValidator = schemaValidator,
        metricReporter = metricReporter,
        replicationInput = replicationInput,
        replicationInputFeatureFlagReader = replicationInputFeatureFlagReader,
      )
    fieldSelector.populateFields(configuredCatalog)
    return fieldSelector
  }

  private fun createRecord(jsonData: String): AirbyteMessage = createRecord(STREAM_NAME, jsonData)

  private fun createRecord(
    streamName: String,
    jsonData: String,
  ): AirbyteMessage =
    AirbyteMessage()
      .withType(AirbyteMessage.Type.RECORD)
      .withRecord(
        AirbyteRecordMessage()
          .withStream(streamName)
          .withData(Jsons.deserialize(jsonData)),
      )
}
