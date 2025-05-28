/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.commands

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.CheckCommandOutputRequest
import io.airbyte.api.client.model.generated.CheckCommandOutputResponse
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
  private val failureConverter: FailureConverter,
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
            commandOutput.failureReason?.let {
              FailureReason()
                .withFailureType(failureConverter.getFailureType(it.failureType))
                .withExternalMessage(it.externalMessage)
                .withStacktrace(it.stacktrace)
                .withInternalMessage(it.internalMessage)
                .withFailureOrigin(failureConverter.getFailureOrigin(it.failureOrigin))
            },
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
}
