/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.commands

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.CommandGetRequest
import io.airbyte.api.client.model.generated.ReplicateCommandOutputRequest
import io.airbyte.api.client.model.generated.ReplicateCommandOutputResponse
import io.airbyte.api.client.model.generated.RunReplicateCommandRequest
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.ReplicationOutput
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.helpers.log
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.WriteOutputCatalogToObjectStorage
import io.airbyte.workers.models.ReplicationApiInput
import io.airbyte.workers.storage.activities.OutputStorageClient
import jakarta.inject.Singleton

@Singleton
class ReplicationCommand(
  airbyteApiClient: AirbyteApiClient,
  private val featureFlagClient: FeatureFlagClient,
  private val catalogStorageClient: OutputStorageClient<ConfiguredAirbyteCatalog>,
  private val failureConverter: FailureConverter,
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

    val replicationOutput = Jsons.`object`(Jsons.jsonNode(commandOutput.attemptSummary), ReplicationOutput::class.java)

    commandOutput.failures?.firstOrNull()

    val output =
      commandOutput.failures
        ?.firstOrNull()
        ?.let {
          ConnectorJobOutput()
            .withOutputType(ConnectorJobOutput.OutputType.REPLICATE)
            .withReplicate(finalizeOutput(id, replicationOutput))
            .withFailureReason(
              FailureReason()
                .withFailureType(failureConverter.getFailureType(it.failureType))
                .withExternalMessage(it.externalMessage)
                .withStacktrace(it.stacktrace)
                .withInternalMessage(it.internalMessage)
                .withFailureOrigin(failureConverter.getFailureOrigin(it.failureOrigin)),
            )
        } ?: ConnectorJobOutput()
        .withOutputType(ConnectorJobOutput.OutputType.REPLICATE)
        .withReplicate(finalizeOutput(id, replicationOutput))

    return output
  }

  fun finalizeOutput(
    commandId: String,
    attemptOutput: ReplicationOutput,
  ): StandardSyncOutput {
    val standardSyncOutput: StandardSyncOutput = reduceReplicationOutput(attemptOutput)

    val commandInput = airbyteApiClient.commandApi.getCommand(CommandGetRequest(commandId)).commandInput
    val replicationApiInput = Jsons.`object`(Jsons.jsonNode(commandInput), ReplicationApiInput::class.java)

    if (featureFlagClient.boolVariation(WriteOutputCatalogToObjectStorage, Connection(replicationApiInput.connectionId))) {
      val uri =
        catalogStorageClient.persist(
          attemptOutput.outputCatalog,
          replicationApiInput.connectionId,
          replicationApiInput.jobId.toLong(),
          replicationApiInput.attemptId.toInt(),
          emptyArray(),
        )

      standardSyncOutput.catalogUri = uri
    }

    val standardSyncOutputString = standardSyncOutput.toString()
    log.debug { "sync summary: $standardSyncOutputString" }

    return standardSyncOutput
  }

  private fun reduceReplicationOutput(output: ReplicationOutput): StandardSyncOutput {
    val standardSyncOutput = StandardSyncOutput()
    val syncSummary = StandardSyncSummary()
    val replicationSummary = output.replicationAttemptSummary

    syncSummary.bytesSynced = replicationSummary.bytesSynced
    syncSummary.recordsSynced = replicationSummary.recordsSynced
    syncSummary.startTime = replicationSummary.startTime
    syncSummary.endTime = replicationSummary.endTime
    syncSummary.status = replicationSummary.status
    syncSummary.totalStats = replicationSummary.totalStats
    syncSummary.streamStats = replicationSummary.streamStats
    syncSummary.performanceMetrics = output.replicationAttemptSummary.performanceMetrics
    syncSummary.streamCount =
      output.outputCatalog.streams.size
        .toLong()

    standardSyncOutput.standardSyncSummary = syncSummary
    standardSyncOutput.failures = output.failures

    return standardSyncOutput
  }
}
