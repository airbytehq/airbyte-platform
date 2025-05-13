/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.commands

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.CheckCommandOutputRequest
import io.airbyte.api.client.model.generated.CheckCommandOutputResponse
import io.airbyte.api.client.model.generated.FailureOrigin
import io.airbyte.api.client.model.generated.FailureType
import io.airbyte.api.client.model.generated.RunCheckCommandRequest
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workers.models.CheckConnectionApiInput
import jakarta.inject.Singleton

@Singleton
class CheckCommandThroughApi(
  airbyteApiClient: AirbyteApiClient,
  private val metricClient: MetricClient,
) : ApiCommandBase<CheckConnectionApiInput>(
    airbyteApiClient = airbyteApiClient,
  ) {
  override val name: String = "check-command-api"

  override fun start(
    input: CheckConnectionApiInput,
    signalPayload: String?,
  ): String {
    val commandId = "check_${input.jobId}_${input.attemptId}_${input.actorId}"

    airbyteApiClient.commandApi.runCheckCommand(
      RunCheckCommandRequest(
        id = commandId,
        actorId = input.actorId,
        jobId = input.jobId,
        attemptNumber = input.attemptId.toInt(),
        signalInput = signalPayload,
      ),
    )

    return commandId
  }

  override fun getOutput(id: String): ConnectorJobOutput {
    val commandOutput: CheckCommandOutputResponse =
      airbyteApiClient.commandApi.getCheckCommandOutput(
        CheckCommandOutputRequest(
          id,
        ),
      )

    val output =
      if (commandOutput.status == CheckCommandOutputResponse.Status.SUCCEEDED) {
        ConnectorJobOutput()
          .withOutputType(ConnectorJobOutput.OutputType.CHECK_CONNECTION)
          .withCheckConnection(
            StandardCheckConnectionOutput()
              .withStatus(StandardCheckConnectionOutput.Status.SUCCEEDED),
          )
      } else {
        ConnectorJobOutput()
          .withOutputType(ConnectorJobOutput.OutputType.CHECK_CONNECTION)
          .withCheckConnection(
            StandardCheckConnectionOutput()
              .withStatus(StandardCheckConnectionOutput.Status.FAILED)
              .withMessage(commandOutput.message),
          ).withFailureReason(
            FailureReason()
              .withFailureType(getFailureType(commandOutput.failureReason?.failureType))
              .withExternalMessage(commandOutput.failureReason?.externalMessage)
              .withStacktrace(commandOutput.failureReason?.stacktrace)
              .withInternalMessage(commandOutput.failureReason?.internalMessage)
              .withFailureOrigin(getFailureOrigin(commandOutput.failureReason?.failureOrigin)),
          )
      }

    metricClient.count(
      metric = OssMetricsRegistry.SIDECAR_CHECK,
      attributes =
        arrayOf(
          MetricAttribute(
            MetricTags.STATUS,
            if (output.checkConnection.status == StandardCheckConnectionOutput.Status.FAILED) "failed" else "success",
          ),
        ),
    )

    return output
  }

  fun getFailureType(failureType: FailureType?): FailureReason.FailureType? {
    if (failureType == null) {
      return FailureReason.FailureType.SYSTEM_ERROR
    }

    return when (failureType) {
      FailureType.CONFIG_ERROR -> FailureReason.FailureType.CONFIG_ERROR
      FailureType.SYSTEM_ERROR -> FailureReason.FailureType.SYSTEM_ERROR
      FailureType.MANUAL_CANCELLATION -> FailureReason.FailureType.MANUAL_CANCELLATION
      FailureType.REFRESH_SCHEMA -> FailureReason.FailureType.REFRESH_SCHEMA
      FailureType.HEARTBEAT_TIMEOUT -> FailureReason.FailureType.HEARTBEAT_TIMEOUT
      FailureType.DESTINATION_TIMEOUT -> FailureReason.FailureType.DESTINATION_TIMEOUT
      FailureType.TRANSIENT_ERROR -> FailureReason.FailureType.TRANSIENT_ERROR
    }
  }

  fun getFailureOrigin(failureOrigin: FailureOrigin?): FailureReason.FailureOrigin {
    if (failureOrigin == null) {
      return FailureReason.FailureOrigin.UNKNOWN
    }

    return when (failureOrigin) {
      FailureOrigin.SOURCE -> FailureReason.FailureOrigin.SOURCE
      FailureOrigin.DESTINATION -> FailureReason.FailureOrigin.DESTINATION
      FailureOrigin.REPLICATION -> FailureReason.FailureOrigin.REPLICATION
      FailureOrigin.PERSISTENCE -> FailureReason.FailureOrigin.PERSISTENCE
      FailureOrigin.AIRBYTE_PLATFORM -> FailureReason.FailureOrigin.AIRBYTE_PLATFORM
      FailureOrigin.NORMALIZATION -> FailureReason.FailureOrigin.NORMALIZATION
      FailureOrigin.DBT -> FailureReason.FailureOrigin.DBT
      FailureOrigin.UNKNOWN -> FailureReason.FailureOrigin.UNKNOWN
    }
  }
}
