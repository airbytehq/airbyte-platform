/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.commands

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.FailureOrigin
import io.airbyte.api.client.model.generated.FailureType
import io.airbyte.api.client.model.generated.ReplicateCommandOutputRequest
import io.airbyte.api.client.model.generated.ReplicateCommandOutputResponse
import io.airbyte.api.client.model.generated.RunReplicateCommandRequest
import io.airbyte.api.client.model.generated.RunReplicateCommandResponse
import io.airbyte.commons.json.Jsons
import io.airbyte.config.AirbyteStream
import io.airbyte.config.CatalogDiff
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.FieldSchemaUpdate
import io.airbyte.config.FieldTransform
import io.airbyte.config.Metadata
import io.airbyte.config.ReplicationAttemptSummary
import io.airbyte.config.StreamAttributePrimaryKeyUpdate
import io.airbyte.config.StreamAttributeTransform
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.StreamTransform
import io.airbyte.config.UpdateStream
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.workers.models.ReplicationApiInput
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNull
import java.util.UUID
import io.airbyte.api.client.model.generated.CatalogDiff as ApiCatalogDiff
import io.airbyte.api.client.model.generated.FieldAdd as ApiFieldAdd
import io.airbyte.api.client.model.generated.FieldRemove as ApiFieldRemove
import io.airbyte.api.client.model.generated.FieldSchemaUpdate as ApiFieldSchemaUpdate
import io.airbyte.api.client.model.generated.FieldTransform as ApiFieldTransform
import io.airbyte.api.client.model.generated.StreamAttributePrimaryKeyUpdate as ApiStreamAttributePrimaryKeyUpdate
import io.airbyte.api.client.model.generated.StreamAttributeTransform as ApiStreamAttributeTransform
import io.airbyte.api.client.model.generated.StreamDescriptor as ApiStreamDescriptor
import io.airbyte.api.client.model.generated.StreamTransform as ApiStreamTransform
import io.airbyte.api.client.model.generated.StreamTransformUpdateStream as ApiStreamTransformUpdateStream

class ReplicationCommandTest {
  private val airbyteApiClient: AirbyteApiClient = mockk(relaxed = true)
  private val featureFlagClient: FeatureFlagClient = mockk(relaxed = true)
  private val failureConverter: FailureConverter = mockk(relaxed = true)
  private val commandApi = airbyteApiClient.commandApi
  private val replicationCommand =
    ReplicationCommand(airbyteApiClient, featureFlagClient, failureConverter)

  private val connectionId = UUID.randomUUID()
  private val jobId = 1L
  private val attemptId = 0L
  private val commandId = "replication_${jobId}_${attemptId}_$connectionId"
  private val workloadId = "workloadId"
  private val workspaceId = UUID.randomUUID()
  private val organizationId = UUID.randomUUID()

  @Test
  fun `start forwards applied catalog diff`() {
    val addFieldSchema = Jsons.deserialize("""{"type":"string"}""")
    val removeFieldSchema = Jsons.deserialize("""{"type":"integer"}""")
    val oldFieldSchema = Jsons.deserialize("""{"type":"number"}""")
    val newFieldSchema = Jsons.deserialize("""{"type":"string"}""")
    val appliedCatalogDiff =
      CatalogDiff().withTransforms(
        listOf(
          StreamTransform()
            .withTransformType(StreamTransform.TransformType.ADD_STREAM)
            .withStreamDescriptor(
              StreamDescriptor()
                .withName("added_stream")
                .withNamespace("source_namespace"),
            ),
          StreamTransform()
            .withTransformType(StreamTransform.TransformType.UPDATE_STREAM)
            .withStreamDescriptor(
              StreamDescriptor()
                .withName("updated_stream")
                .withNamespace("source_namespace"),
            ).withUpdateStream(
              UpdateStream()
                .withFieldTransforms(
                  listOf(
                    FieldTransform()
                      .withTransformType(FieldTransform.TransformType.ADD_FIELD)
                      .withFieldName(listOf("profile", "nickname"))
                      .withBreaking(false)
                      .withAddField(addFieldSchema),
                    FieldTransform()
                      .withTransformType(FieldTransform.TransformType.REMOVE_FIELD)
                      .withFieldName(listOf("legacy_id"))
                      .withBreaking(true)
                      .withRemoveField(removeFieldSchema),
                    FieldTransform()
                      .withTransformType(FieldTransform.TransformType.UPDATE_FIELD_SCHEMA)
                      .withFieldName(listOf("score"))
                      .withBreaking(true)
                      .withUpdateFieldSchema(
                        FieldSchemaUpdate()
                          .withOldSchema(oldFieldSchema)
                          .withNewSchema(newFieldSchema),
                      ),
                  ),
                ).withStreamAttributeTransforms(
                  listOf(
                    StreamAttributeTransform()
                      .withTransformType(StreamAttributeTransform.TransformType.UPDATE_PRIMARY_KEY)
                      .withBreaking(true)
                      .withUpdatePrimaryKey(
                        StreamAttributePrimaryKeyUpdate()
                          .withOldPrimaryKey(listOf(listOf("id")))
                          .withNewPrimaryKey(listOf(listOf("tenant_id"), listOf("id"))),
                      ),
                  ),
                ),
            ),
        ),
      )
    val expectedCatalogDiff =
      ApiCatalogDiff(
        transforms =
          listOf(
            ApiStreamTransform(
              transformType = ApiStreamTransform.TransformType.ADD_STREAM,
              streamDescriptor = ApiStreamDescriptor(name = "added_stream", namespace = "source_namespace"),
            ),
            ApiStreamTransform(
              transformType = ApiStreamTransform.TransformType.UPDATE_STREAM,
              streamDescriptor = ApiStreamDescriptor(name = "updated_stream", namespace = "source_namespace"),
              updateStream =
                ApiStreamTransformUpdateStream(
                  fieldTransforms =
                    listOf(
                      ApiFieldTransform(
                        transformType = ApiFieldTransform.TransformType.ADD_FIELD,
                        fieldName = listOf("profile", "nickname"),
                        breaking = false,
                        addField = ApiFieldAdd(schema = addFieldSchema),
                      ),
                      ApiFieldTransform(
                        transformType = ApiFieldTransform.TransformType.REMOVE_FIELD,
                        fieldName = listOf("legacy_id"),
                        breaking = true,
                        removeField = ApiFieldRemove(schema = removeFieldSchema),
                      ),
                      ApiFieldTransform(
                        transformType = ApiFieldTransform.TransformType.UPDATE_FIELD_SCHEMA,
                        fieldName = listOf("score"),
                        breaking = true,
                        updateFieldSchema =
                          ApiFieldSchemaUpdate(
                            oldSchema = oldFieldSchema,
                            newSchema = newFieldSchema,
                          ),
                      ),
                    ),
                  streamAttributeTransforms =
                    listOf(
                      ApiStreamAttributeTransform(
                        transformType = ApiStreamAttributeTransform.TransformType.UPDATE_PRIMARY_KEY,
                        breaking = true,
                        updatePrimaryKey =
                          ApiStreamAttributePrimaryKeyUpdate(
                            oldPrimaryKey = listOf(listOf("id")),
                            newPrimaryKey = listOf(listOf("tenant_id"), listOf("id")),
                          ),
                      ),
                    ),
                ),
            ),
          ),
      )
    val request = slot<RunReplicateCommandRequest>()
    every { commandApi.runReplicateCommand(capture(request)) } returns RunReplicateCommandResponse(commandId)

    replicationCommand.start(
      ReplicationApiInput(
        connectionId = connectionId,
        jobId = jobId.toString(),
        attemptId = attemptId,
        appliedCatalogDiff = appliedCatalogDiff,
      ),
      signalPayload = "signal payload",
    )

    assertEquals(expectedCatalogDiff, request.captured.appliedCatalogDiff)
  }

  @Test
  fun `start preserves null applied catalog diff`() {
    val request = slot<RunReplicateCommandRequest>()
    every { commandApi.runReplicateCommand(capture(request)) } returns RunReplicateCommandResponse(commandId)

    replicationCommand.start(
      ReplicationApiInput(
        connectionId = connectionId,
        jobId = jobId.toString(),
        attemptId = attemptId,
        appliedCatalogDiff = null,
      ),
      signalPayload = null,
    )

    assertNull(request.captured.appliedCatalogDiff)
  }

  @Test
  fun `getOutput should return replicate output on success`() {
    val outputCatalog =
      ConfiguredAirbyteCatalog()
        .withStreams(
          listOf(
            ConfiguredAirbyteStream(
              AirbyteStream(
                name = "stream1",
                jsonSchema = Jsons.emptyObject(),
                supportedSyncModes = listOf(io.airbyte.config.SyncMode.FULL_REFRESH),
              ),
            ),
          ),
        )
    val replicationAttemptSummary =
      ReplicationAttemptSummary()
        .withBytesSynced(100L)
        .withRecordsSynced(10L)
        .withStartTime(0L)
        .withEndTime(1000L)
        .withStatus(io.airbyte.config.StandardSyncSummary.ReplicationStatus.COMPLETED)

    val attemptSummary = Jsons.jsonNode(replicationAttemptSummary)
    val replicateCommandOutputResponse =
      ReplicateCommandOutputResponse(
        commandId,
        attemptSummary,
        null,
      )

    every { commandApi.getReplicateCommandOutput(ReplicateCommandOutputRequest(commandId)) } returns replicateCommandOutputResponse

    val expectedOutput =
      ConnectorJobOutput()
        .withOutputType(ConnectorJobOutput.OutputType.REPLICATE)
        .withReplicate(replicationCommand.finalizeOutput(commandId, replicationAttemptSummary, null))

    val actualOutput = replicationCommand.getOutput(commandId)

    assertEquals(expectedOutput.outputType, actualOutput.outputType)
    assertEquals(expectedOutput.replicate?.standardSyncSummary?.bytesSynced, actualOutput.replicate?.standardSyncSummary?.bytesSynced)
    assertEquals(expectedOutput.replicate?.standardSyncSummary?.recordsSynced, actualOutput.replicate?.standardSyncSummary?.recordsSynced)
    assertEquals(expectedOutput.replicate?.failures, actualOutput.replicate?.failures)
    assertEquals(expectedOutput.failureReason, actualOutput.failureReason)
    assertEquals(expectedOutput.replicate?.standardSyncSummary?.performanceMetrics, actualOutput.replicate?.standardSyncSummary?.performanceMetrics)
    assertEquals(expectedOutput.replicate?.standardSyncSummary?.streamCount, actualOutput.replicate?.standardSyncSummary?.streamCount)
  }

  @Test
  fun `getOutput should return replicate output with failure reason on failure`() {
    val externalMessage = "External error message"

    val replicationAttemptSummary =
      ReplicationAttemptSummary()
        .withBytesSynced(100L)
        .withRecordsSynced(10L)
        .withStartTime(0L)
        .withEndTime(1000L)
        .withStatus(io.airbyte.config.StandardSyncSummary.ReplicationStatus.COMPLETED)

    val attemptSummary = Jsons.jsonNode(replicationAttemptSummary) as com.fasterxml.jackson.databind.node.ObjectNode
    val failureReason =
      io.airbyte.api.client.model.generated.FailureReason(
        timestamp = System.currentTimeMillis(),
        failureType = FailureType.SYSTEM_ERROR,
        externalMessage = externalMessage,
      )
    val replicateCommandOutputResponse = ReplicateCommandOutputResponse(commandId, attemptSummary, listOf(failureReason))

    every { commandApi.getReplicateCommandOutput(ReplicateCommandOutputRequest(commandId)) } returns replicateCommandOutputResponse
    every { failureConverter.getFailureType(FailureType.SYSTEM_ERROR) } returns FailureReason.FailureType.SYSTEM_ERROR

    val expectedFailureReason =
      FailureReason()
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)
        .withExternalMessage(externalMessage)

    val expectedOutput =
      ConnectorJobOutput()
        .withOutputType(ConnectorJobOutput.OutputType.REPLICATE)
        .withReplicate(replicationCommand.finalizeOutput(commandId, replicationAttemptSummary, null))
        .withFailureReason(expectedFailureReason)

    val actualOutput = replicationCommand.getOutput(commandId)

    assertEquals(expectedOutput.failureReason?.failureType, actualOutput.failureReason?.failureType)
    assertEquals(expectedOutput.failureReason?.externalMessage, actualOutput.failureReason?.externalMessage)
  }

  @Test
  fun `apiFailureReasonToConfigModel returns all fields`() {
    every { failureConverter.getFailureOrigin(FailureOrigin.SOURCE) } returns FailureReason.FailureOrigin.SOURCE
    every { failureConverter.getFailureType(FailureType.CONFIG_ERROR) } returns FailureReason.FailureType.CONFIG_ERROR
    val input =
      io.airbyte.api.client.model.generated.FailureReason(
        timestamp = 42,
        FailureOrigin.SOURCE,
        FailureType.CONFIG_ERROR,
        externalMessage = "example external message",
        internalMessage = "example internal message",
        stacktrace = "example stacktrace",
        retryable = true,
        fromTraceMessage = true,
        ApiStreamDescriptor(name = "example name", namespace = "example namespace"),
      )

    val output = replicationCommand.apiFailureReasonToConfigModel(input)

    assertEquals(
      FailureReason()
        .withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
        .withFailureType(FailureReason.FailureType.CONFIG_ERROR)
        .withInternalMessage("example internal message")
        .withExternalMessage("example external message")
        .withMetadata(Metadata().withAdditionalProperty("from_trace_message", true))
        .withStacktrace("example stacktrace")
        .withRetryable(true)
        .withTimestamp(42)
        .withStreamDescriptor(StreamDescriptor().withName("example name").withNamespace("example namespace")),
      output,
    )
  }

  @Test
  fun `apiFailureReasonToConfigModel handles null fields`() {
    // getFailureOrigin is required to return nonnull value
    every { failureConverter.getFailureOrigin(null) } returns FailureReason.FailureOrigin.SOURCE
    every { failureConverter.getFailureType(null) } returns null
    val input =
      io.airbyte.api.client.model.generated.FailureReason(
        // timestamp isn't nullable
        timestamp = 42,
        failureOrigin = null,
        failureType = null,
        externalMessage = null,
        internalMessage = null,
        stacktrace = null,
        retryable = null,
        fromTraceMessage = null,
        streamDescriptor = null,
      )

    val output = replicationCommand.apiFailureReasonToConfigModel(input)

    assertEquals(
      FailureReason()
        .withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
        .withTimestamp(42)
        // We always populate a Metadata blob, even when we don't write anything into it
        .withMetadata(Metadata()),
      output,
    )
  }

  @Test
  fun `getOutput should handle a null ReplicationAttemptSummary 2`() {
    val externalMessage = "External error message"

    val failureReason =
      io.airbyte.api.client.model.generated.FailureReason(
        timestamp = System.currentTimeMillis(),
        failureType = FailureType.SYSTEM_ERROR,
        externalMessage = externalMessage,
      )
    val replicateCommandOutputResponse = ReplicateCommandOutputResponse(commandId, null, listOf(failureReason))

    every { commandApi.getReplicateCommandOutput(ReplicateCommandOutputRequest(commandId)) } returns replicateCommandOutputResponse
    every { failureConverter.getFailureType(FailureType.SYSTEM_ERROR) } returns FailureReason.FailureType.SYSTEM_ERROR

    val expectedFailureReason =
      FailureReason()
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)
        .withExternalMessage(externalMessage)

    val expectedOutput =
      ConnectorJobOutput()
        .withOutputType(ConnectorJobOutput.OutputType.REPLICATE)
        .withFailureReason(expectedFailureReason)

    val actualOutput = replicationCommand.getOutput(commandId)

    assertEquals(expectedOutput.failureReason?.failureType, actualOutput.failureReason?.failureType)
    assertEquals(expectedOutput.failureReason?.externalMessage, actualOutput.failureReason?.externalMessage)
  }

  @Test
  fun `getOutput should handle a null ReplicationAttemptSummary`() {
    val replicationAttemptSummary =
      ReplicationAttemptSummary()
        .withBytesSynced(100L)
        .withRecordsSynced(10L)
        .withStartTime(0L)
        .withEndTime(1000L)
        .withStatus(io.airbyte.config.StandardSyncSummary.ReplicationStatus.COMPLETED)

    val replicateCommandOutputResponse =
      ReplicateCommandOutputResponse(
        commandId,
        null,
        null,
      )

    every { commandApi.getReplicateCommandOutput(ReplicateCommandOutputRequest(commandId)) } returns replicateCommandOutputResponse

    val expectedOutput =
      ConnectorJobOutput()
        .withOutputType(ConnectorJobOutput.OutputType.REPLICATE)
        .withReplicate(replicationCommand.finalizeOutput(commandId, replicationAttemptSummary, null))

    val actualOutput = replicationCommand.getOutput(commandId)

    assertEquals(expectedOutput.outputType, actualOutput.outputType)
    assertNull(actualOutput.replicate?.standardSyncSummary?.bytesSynced)
    assertNull(actualOutput.replicate?.standardSyncSummary?.recordsSynced)
    assertEquals(expectedOutput.replicate?.failures, actualOutput.replicate?.failures)
    assertEquals(expectedOutput.failureReason, actualOutput.failureReason)
    assertEquals(expectedOutput.replicate?.standardSyncSummary?.performanceMetrics, actualOutput.replicate?.standardSyncSummary?.performanceMetrics)
    assertEquals(expectedOutput.replicate?.standardSyncSummary?.streamCount, actualOutput.replicate?.standardSyncSummary?.streamCount)
  }
}
