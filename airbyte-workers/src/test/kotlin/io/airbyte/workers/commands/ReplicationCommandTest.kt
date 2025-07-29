/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.commands

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.CommandGetRequest
import io.airbyte.api.client.model.generated.CommandGetResponse
import io.airbyte.api.client.model.generated.FailureOrigin
import io.airbyte.api.client.model.generated.FailureType
import io.airbyte.api.client.model.generated.ReplicateCommandOutputRequest
import io.airbyte.api.client.model.generated.ReplicateCommandOutputResponse
import io.airbyte.commons.converters.CatalogClientConverters
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.temporal.scheduling.ReplicationCommandApiInput
import io.airbyte.config.ActivityPayloadURI
import io.airbyte.config.AirbyteStream
import io.airbyte.config.CatalogDiff
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.Metadata
import io.airbyte.config.ReplicationAttemptSummary
import io.airbyte.config.StreamDescriptor
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.workers.storage.activities.OutputStorageClient
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNull
import java.util.UUID
import io.airbyte.api.client.model.generated.StreamDescriptor as ApiStreamDescriptor

class ReplicationCommandTest {
  private val airbyteApiClient: AirbyteApiClient = mockk(relaxed = true)
  private val featureFlagClient: FeatureFlagClient = mockk(relaxed = true)
  private val catalogStorageClient: OutputStorageClient<ConfiguredAirbyteCatalog> = mockk(relaxed = true)
  private val failureConverter: FailureConverter = mockk(relaxed = true)
  private val catalogClientConverters: CatalogClientConverters = mockk(relaxed = true)
  private val commandApi = airbyteApiClient.commandApi
  private val replicationCommand =
    ReplicationCommand(airbyteApiClient, featureFlagClient, catalogStorageClient, failureConverter, catalogClientConverters)

  private val connectionId = UUID.randomUUID()
  private val jobId = 1L
  private val attemptId = 0L
  private val commandId = "replication_${jobId}_${attemptId}_$connectionId"
  private val workloadId = "workloadId"
  private val workspaceId = UUID.randomUUID()
  private val organizationId = UUID.randomUUID()

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

    // Mock the catalog storage
    val expectedUri = "s3://bucket/path/to/catalog.json"
    val expectedActivityPayloadURI = ActivityPayloadURI().withId(expectedUri)
    every {
      catalogStorageClient.persist(any(), connectionId, jobId, attemptId.toInt(), emptyArray())
    } returns expectedActivityPayloadURI

    every {
      commandApi.getCommand(CommandGetRequest(commandId))
    } returns
      CommandGetResponse(
        id = commandId,
        workspaceId = workspaceId,
        commandType = "replicate",
        commandInput =
          Jsons
            .serialize(
              ReplicationCommandApiInput.ReplicationApiInput(connectionId, jobId.toString(), attemptId, CatalogDiff()),
            ),
        workloadId = workloadId,
        organizationId = organizationId,
        createdAt = java.time.OffsetDateTime.now(),
        updatedAt = java.time.OffsetDateTime.now(),
      )
    val expectedOutput =
      ConnectorJobOutput()
        .withOutputType(ConnectorJobOutput.OutputType.REPLICATE)
        .withReplicate(replicationCommand.finalizeOutput(commandId, replicationAttemptSummary, null, null))

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

    // Mock the catalog storage
    val expectedUri = "s3://bucket/path/to/catalog.json"
    val expectedActivityPayloadURI = ActivityPayloadURI().withId(expectedUri)
    every {
      catalogStorageClient.persist(any(), connectionId, jobId, attemptId.toInt(), emptyArray())
    } returns expectedActivityPayloadURI

    every {
      commandApi.getCommand(CommandGetRequest(commandId))
    } returns
      CommandGetResponse(
        id = commandId,
        workspaceId = workspaceId,
        commandType = "replicate",
        commandInput =
          Jsons
            .serialize(
              ReplicationCommandApiInput.ReplicationApiInput(connectionId, jobId.toString(), attemptId, CatalogDiff()),
            ),
        workloadId = workloadId,
        organizationId = organizationId,
        createdAt = java.time.OffsetDateTime.now(),
        updatedAt = java.time.OffsetDateTime.now(),
      )

    val expectedFailureReason =
      FailureReason()
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)
        .withExternalMessage(externalMessage)

    val expectedOutput =
      ConnectorJobOutput()
        .withOutputType(ConnectorJobOutput.OutputType.REPLICATE)
        .withReplicate(replicationCommand.finalizeOutput(commandId, replicationAttemptSummary, null, null))
        .withFailureReason(expectedFailureReason)

    val actualOutput = replicationCommand.getOutput(commandId)

    assertEquals(expectedOutput.failureReason?.failureType, actualOutput.failureReason?.failureType)
    assertEquals(expectedOutput.failureReason?.externalMessage, actualOutput.failureReason?.externalMessage)
  }

  @Test
  fun `finalizeOutput should persist catalog to object storage`() {
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

    val expectedUri = "s3://bucket/path/to/catalog.json"
    val expectedActivityPayloadURI = ActivityPayloadURI().withId(expectedUri)

    every {
      catalogStorageClient.persist(outputCatalog, connectionId, jobId, attemptId.toInt(), emptyArray())
    } returns expectedActivityPayloadURI

    every {
      commandApi.getCommand(CommandGetRequest(commandId))
    } returns
      CommandGetResponse(
        id = commandId,
        workspaceId = workspaceId,
        commandType = "replicate",
        commandInput =
          Jsons
            .serialize(
              ReplicationCommandApiInput.ReplicationApiInput(connectionId, jobId.toString(), attemptId, CatalogDiff()),
            ),
        workloadId = workloadId,
        organizationId = organizationId,
        createdAt = java.time.OffsetDateTime.now(),
        updatedAt = java.time.OffsetDateTime.now(),
      )
    val output = replicationCommand.finalizeOutput(commandId, replicationAttemptSummary, outputCatalog, null)

    verify {
      catalogStorageClient.persist(outputCatalog, connectionId, jobId, attemptId.toInt(), emptyArray())
    }

    assertEquals(expectedActivityPayloadURI, output.catalogUri)
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

    // Mock the catalog storage
    val expectedUri = "s3://bucket/path/to/catalog.json"
    val expectedActivityPayloadURI = ActivityPayloadURI().withId(expectedUri)
    every {
      catalogStorageClient.persist(any(), connectionId, jobId, attemptId.toInt(), emptyArray())
    } returns expectedActivityPayloadURI

    every {
      commandApi.getCommand(CommandGetRequest(commandId))
    } returns
      CommandGetResponse(
        id = commandId,
        workspaceId = workspaceId,
        commandType = "replicate",
        commandInput =
          Jsons
            .serialize(
              ReplicationCommandApiInput.ReplicationApiInput(connectionId, jobId.toString(), attemptId, CatalogDiff()),
            ),
        workloadId = workloadId,
        organizationId = organizationId,
        createdAt = java.time.OffsetDateTime.now(),
        updatedAt = java.time.OffsetDateTime.now(),
      )

    val expectedFailureReason =
      FailureReason()
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)
        .withExternalMessage(externalMessage)

    val expectedOutput =
      ConnectorJobOutput()
        .withOutputType(ConnectorJobOutput.OutputType.REPLICATE)
//        .withReplicate(replicationCommand.finalizeOutput(commandId, replicationAttemptSummary, null, null))
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

    // Mock the catalog storage
    val expectedUri = "s3://bucket/path/to/catalog.json"
    val expectedActivityPayloadURI = ActivityPayloadURI().withId(expectedUri)
    every {
      catalogStorageClient.persist(any(), connectionId, jobId, attemptId.toInt(), emptyArray())
    } returns expectedActivityPayloadURI

    every {
      commandApi.getCommand(CommandGetRequest(commandId))
    } returns
      CommandGetResponse(
        id = commandId,
        workspaceId = workspaceId,
        commandType = "replicate",
        commandInput =
          Jsons
            .serialize(
              ReplicationCommandApiInput.ReplicationApiInput(connectionId, jobId.toString(), attemptId, CatalogDiff()),
            ),
        workloadId = workloadId,
        organizationId = organizationId,
        createdAt = java.time.OffsetDateTime.now(),
        updatedAt = java.time.OffsetDateTime.now(),
      )
    val expectedOutput =
      ConnectorJobOutput()
        .withOutputType(ConnectorJobOutput.OutputType.REPLICATE)
        .withReplicate(replicationCommand.finalizeOutput(commandId, replicationAttemptSummary, null, null))

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
