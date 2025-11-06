/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.commands

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.RunSpecCommandRequest
import io.airbyte.api.client.model.generated.SpecCommandOutputRequest
import io.airbyte.api.client.model.generated.SpecCommandOutputResponse
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.workers.models.SpecApiInput
import jakarta.inject.Singleton

@Singleton
class SpecCommandV2(
  airbyteApiClient: AirbyteApiClient,
  private val failureConverter: FailureConverter,
) : ApiCommandBase<SpecApiInput>(
    airbyteApiClient = airbyteApiClient,
  ) {
  override val name: String = "spec-command-api"

  override fun start(
    input: SpecApiInput,
    signalPayload: String?,
  ): String {
    val commandId = input.commandId ?: "spec_${input.requestId}"

    airbyteApiClient.commandApi.runSpecCommand(
      RunSpecCommandRequest(
        id = commandId,
        workspaceId = input.workspaceId,
        dockerImageTag = input.dockerImageTag,
        dockerImage = input.dockerImage,
        actorDefinitionId = input.actorDefinitionId,
        signalInput = signalPayload,
      ),
    )

    return commandId
  }

  override fun getOutput(id: String): ConnectorJobOutput {
    val commandOutput: SpecCommandOutputResponse =
      airbyteApiClient.commandApi.getSpecCommandOutput(
        SpecCommandOutputRequest(id),
      )

    val output =
      if (commandOutput.status == SpecCommandOutputResponse.Status.SUCCEEDED) {
        ConnectorJobOutput()
          .withOutputType(ConnectorJobOutput.OutputType.SPEC)
          .withSpec(commandOutput.spec as? ConnectorSpecification)
      } else {
        ConnectorJobOutput()
          .withOutputType(ConnectorJobOutput.OutputType.SPEC)
          .withSpec(null)
          .withFailureReason(
            commandOutput.failureReason?.let {
              FailureReason()
                .withFailureType(failureConverter.getFailureType(it.failureType))
                .withExternalMessage(it.externalMessage)
                .withStacktrace(it.stacktrace)
                .withInternalMessage(it.internalMessage)
                .withFailureOrigin(failureConverter.getFailureOrigin(it.failureOrigin))
                .withTimestamp(it.timestamp)
            },
          )
      }
    return output
  }
}
