/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.commands

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.DiscoverCommandOutputRequest
import io.airbyte.api.client.model.generated.DiscoverCommandOutputResponse
import io.airbyte.api.client.model.generated.RunDiscoverCommandRequest
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workers.models.DiscoverSourceApiInput
import jakarta.inject.Singleton

@Singleton
class DiscoverCommandV2(
  airbyteApiClient: AirbyteApiClient,
  private val metricClient: MetricClient,
  private val failureConverter: FailureConverter,
) : ApiCommandBase<DiscoverSourceApiInput>(
    airbyteApiClient = airbyteApiClient,
  ) {
  override val name: String = "discover-command-api"

  override fun start(
    input: DiscoverSourceApiInput,
    signalPayload: String?,
  ): String {
    val commandId = "discover_${input.jobId}_${input.attemptId}_${input.actorId}"

    airbyteApiClient.commandApi.runDiscoverCommand(
      RunDiscoverCommandRequest(
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
    val commandOutput = airbyteApiClient.commandApi.getDiscoverCommandOutput(DiscoverCommandOutputRequest(id))

    val output: ConnectorJobOutput =
      if (commandOutput.status == DiscoverCommandOutputResponse.Status.SUCCEEDED) {
        ConnectorJobOutput()
          .withOutputType(ConnectorJobOutput.OutputType.DISCOVER_CATALOG_ID)
          .withDiscoverCatalogId(commandOutput.catalogId)
      } else {
        ConnectorJobOutput()
          .withOutputType(ConnectorJobOutput.OutputType.DISCOVER_CATALOG_ID)
          .withDiscoverCatalogId(null)
          .withFailureReason(
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
      metric = OssMetricsRegistry.CATALOG_DISCOVERY,
      attributes =
        arrayOf(
          MetricAttribute(
            MetricTags.STATUS,
            if (output.discoverCatalogId == null) "failed" else "success",
          ),
        ),
    )

    return output
  }
}
