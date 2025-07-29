/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.commands

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.CommandGetRequest
import io.airbyte.api.client.model.generated.ReplicateCommandOutputRequest
import io.airbyte.api.client.model.generated.ReplicateCommandOutputResponse
import io.airbyte.api.client.model.generated.RunReplicateCommandRequest
import io.airbyte.commons.converters.ApiClientConverters.Companion.toInternal
import io.airbyte.commons.converters.CatalogClientConverters
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.temporal.scheduling.ReplicationCommandApiInput
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.Metadata
import io.airbyte.config.ReplicationAttemptSummary
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.helpers.log
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.workers.helper.TRACE_MESSAGE_METADATA_KEY
import io.airbyte.workers.models.ReplicationApiInput
import io.airbyte.workers.storage.activities.OutputStorageClient
import jakarta.inject.Singleton

@Singleton
class ReplicationCommand(
  airbyteApiClient: AirbyteApiClient,
  private val featureFlagClient: FeatureFlagClient,
  private val catalogStorageClient: OutputStorageClient<ConfiguredAirbyteCatalog>,
  private val failureConverter: FailureConverter,
  private val catalogClientConverters: CatalogClientConverters,
) : ApiCommandBase<ReplicationApiInput>(airbyteApiClient) {
  override val name = "replication-command-api"

  override fun start(
    input: ReplicationApiInput,
    signalPayload: String?,
  ): String {
    val commandId = "replication_${input.jobId}_${input.attemptId}_${input.connectionId}"

    airbyteApiClient.commandApi.runReplicateCommand(
      RunReplicateCommandRequest(
        id = commandId,
        connectionId = input.connectionId,
        jobId = input.jobId,
        attemptNumber = input.attemptId.toInt(),
        signalInput = signalPayload,
      ),
    )

    return commandId
  }

  override fun getOutput(id: String): ConnectorJobOutput {
    val commandOutput: ReplicateCommandOutputResponse =
      airbyteApiClient.commandApi.getReplicateCommandOutput(
        ReplicateCommandOutputRequest(
          id,
        ),
      )

    val catalog: ConfiguredAirbyteCatalog? = commandOutput.catalog?.let { catalogClientConverters.toConfiguredAirbyteInternal(it) }

    val replicationAttemptSummary: ReplicationAttemptSummary =
      Jsons.`object`(
        Jsons.jsonNode(commandOutput.attemptSummary ?: mapOf<Any, Any>()),
        ReplicationAttemptSummary::class.java,
      )

    val failures: List<FailureReason>? = commandOutput.failures?.map { apiFailureReasonToConfigModel(it) }

    val output =
      failures
        ?.firstOrNull()
        ?.let {
          ConnectorJobOutput()
            .withOutputType(ConnectorJobOutput.OutputType.REPLICATE)
            .withReplicate(finalizeOutput(id, replicationAttemptSummary, catalog, failures))
            .withFailureReason(it)
        } ?: ConnectorJobOutput()
        .withOutputType(ConnectorJobOutput.OutputType.REPLICATE)
        .withReplicate(finalizeOutput(id, replicationAttemptSummary, catalog, failures))

    return output
  }

  internal fun apiFailureReasonToConfigModel(apiFailureReason: io.airbyte.api.client.model.generated.FailureReason) =
    FailureReason()
      .withFailureType(failureConverter.getFailureType(apiFailureReason.failureType))
      .withExternalMessage(apiFailureReason.externalMessage)
      .withStacktrace(apiFailureReason.stacktrace)
      .withInternalMessage(apiFailureReason.internalMessage)
      .withFailureOrigin(failureConverter.getFailureOrigin(apiFailureReason.failureOrigin))
      .withTimestamp(apiFailureReason.timestamp)
      .withRetryable(apiFailureReason.retryable)
      .withMetadata(
        Metadata().apply {
          apiFailureReason.fromTraceMessage?.let { fromTraceMessage ->
            this.setAdditionalProperty(TRACE_MESSAGE_METADATA_KEY, fromTraceMessage)
          }
        },
      ).apply {
        apiFailureReason.streamDescriptor?.let { apiStreamDescriptor ->
          this.streamDescriptor = apiStreamDescriptor.toInternal()
        }
      }

  fun finalizeOutput(
    commandId: String,
    replicationAttemptSummary: ReplicationAttemptSummary,
    catalog: ConfiguredAirbyteCatalog?,
    failures: List<FailureReason>?,
  ): StandardSyncOutput {
    val standardSyncOutput: StandardSyncOutput = reduceReplicationOutput(replicationAttemptSummary, catalog, failures)

    val commandInput = airbyteApiClient.commandApi.getCommand(CommandGetRequest(commandId)).commandInput

    val replicationApiInput = Jsons.deserialize(commandInput.toString(), ReplicationCommandApiInput.ReplicationApiInput::class.java)

    val uri =
      catalogStorageClient.persist(
        catalog,
        replicationApiInput.connectionId,
        replicationApiInput.jobId.toLong(),
        replicationApiInput.attemptId.toInt(),
        emptyArray(),
      )
    standardSyncOutput.catalogUri = uri

    val standardSyncOutputString = standardSyncOutput.toString()
    log.debug { "sync summary: $standardSyncOutputString" }

    return standardSyncOutput
  }

  private fun reduceReplicationOutput(
    replicationAttemptSummary: ReplicationAttemptSummary,
    catalog: ConfiguredAirbyteCatalog?,
    failures: List<FailureReason>?,
  ): StandardSyncOutput {
    val standardSyncOutput = StandardSyncOutput()
    val syncSummary = StandardSyncSummary()

    syncSummary.bytesSynced = replicationAttemptSummary.bytesSynced
    syncSummary.recordsSynced = replicationAttemptSummary.recordsSynced
    syncSummary.startTime = replicationAttemptSummary.startTime
    syncSummary.endTime = replicationAttemptSummary.endTime
    syncSummary.status = replicationAttemptSummary.status
    syncSummary.totalStats = replicationAttemptSummary.totalStats
    syncSummary.streamStats = replicationAttemptSummary.streamStats
    syncSummary.performanceMetrics = replicationAttemptSummary?.performanceMetrics
    syncSummary.streamCount = catalog?. let { it.streams.size.toLong() } ?: 0L
    standardSyncOutput.standardSyncSummary = syncSummary
    standardSyncOutput.failures = failures

    return standardSyncOutput
  }
}
