/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.CommandApi
import io.airbyte.api.model.generated.CancelCommandRequest
import io.airbyte.api.model.generated.CancelCommandResponse
import io.airbyte.api.model.generated.CheckCommandOutputRequest
import io.airbyte.api.model.generated.CheckCommandOutputResponse
import io.airbyte.api.model.generated.CommandStatusRequest
import io.airbyte.api.model.generated.CommandStatusResponse
import io.airbyte.api.model.generated.RunCheckCommandRequest
import io.airbyte.api.model.generated.RunCheckCommandResponse
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.enums.toEnum
import io.airbyte.commons.server.helpers.SecretSanitizer
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.config.WorkloadPriority
import io.airbyte.protocol.models.Jsons
import io.airbyte.server.services.CommandService
import io.micronaut.context.annotation.Context
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import io.airbyte.api.model.generated.FailureReason as ApiFailureReason

@Controller("/api/v1/commands")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
class CommandApiController(
  private val commandService: CommandService,
  private val secretSanitizer: SecretSanitizer,
) : CommandApi {
  @Post("/cancel")
  @Secured(AuthRoleConstants.WORKSPACE_RUNNER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun cancelCommand(
    @Body cancelCommandRequest: CancelCommandRequest,
  ): CancelCommandResponse {
    commandService.cancel(cancelCommandRequest.id)
    return CancelCommandResponse().id(cancelCommandRequest.id)
  }

  @Post("/output/check")
  @Secured(AuthRoleConstants.WORKSPACE_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getCheckCommandOutput(
    @Body checkCommandOutputRequest: CheckCommandOutputRequest,
  ): CheckCommandOutputResponse {
    val output = commandService.getConnectorJobOutput(checkCommandOutputRequest.id)
    return CheckCommandOutputResponse().apply {
      id(checkCommandOutputRequest.id)
      output?.let {
        status(it.checkConnection.status.toApi())
        failureReason(it.failureReason.toApi())
      }
    }
  }

  private fun FailureReason?.toApi(): ApiFailureReason? =
    when (this) {
      null -> null
      else ->
        ApiFailureReason()
          .failureOrigin(this.failureOrigin.convertTo())
          .failureType(this.failureType.convertTo())
          .externalMessage(this.externalMessage)
          .internalMessage(this.internalMessage)
          .stacktrace(this.stacktrace)
          .timestamp(this.timestamp)
          .retryable(if (this.retryable != null) this.retryable else true)
    }

  private fun StandardCheckConnectionOutput.Status.toApi(): CheckCommandOutputResponse.StatusEnum =
    when (this) {
      StandardCheckConnectionOutput.Status.SUCCEEDED -> CheckCommandOutputResponse.StatusEnum.SUCCEEDED
      StandardCheckConnectionOutput.Status.FAILED -> CheckCommandOutputResponse.StatusEnum.FAILED
    }

  @Post("/status")
  @Secured(AuthRoleConstants.WORKSPACE_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getCommandStatus(
    @Body commandStatusRequest: CommandStatusRequest,
  ): CommandStatusResponse {
    val commandStatus = commandService.getStatus(commandStatusRequest.id)
    return CommandStatusResponse().apply {
      id(commandStatusRequest.id)
      commandStatus?.let { status(it.convertTo()) }
    }
  }

  @Post("/run/check")
  @Secured(AuthRoleConstants.WORKSPACE_RUNNER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun runCheckCommand(
    @Body runCheckCommandRequest: RunCheckCommandRequest,
  ): RunCheckCommandResponse {
    val priority: WorkloadPriority = runCheckCommandRequest.priority?.uppercase()?.toEnum<WorkloadPriority>() ?: WorkloadPriority.DEFAULT
    if (runCheckCommandRequest.actorId != null) {
      commandService.createCheckCommand(
        commandId = runCheckCommandRequest.id,
        actorId = runCheckCommandRequest.actorId,
        jobId = runCheckCommandRequest.jobId,
        attemptNumber = runCheckCommandRequest.attemptNumber?.toLong(),
        workloadPriority = priority,
        signalInput = runCheckCommandRequest.signalInput,
        commandInput = runCheckCommandRequest,
      )
    } else {
      val sanitizedConfig =
        secretSanitizer.sanitizePartialConfig(
          actorDefinitionId = runCheckCommandRequest.actorDefinitionId,
          workspaceId = runCheckCommandRequest.workspaceId,
          connectionConfiguration = Jsons.jsonNode(runCheckCommandRequest.config),
        )
      runCheckCommandRequest.config = sanitizedConfig
      commandService.createCheckCommand(
        commandId = runCheckCommandRequest.id,
        actorDefinitionId = runCheckCommandRequest.actorDefinitionId,
        workspaceId = runCheckCommandRequest.workspaceId,
        configuration = sanitizedConfig,
        workloadPriority = priority,
        signalInput = runCheckCommandRequest.signalInput,
        commandInput = runCheckCommandRequest,
      )
    }
    return RunCheckCommandResponse().id(runCheckCommandRequest.id)
  }
}
