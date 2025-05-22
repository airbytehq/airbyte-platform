/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.CommandApi
import io.airbyte.api.model.generated.CancelCommandRequest
import io.airbyte.api.model.generated.CancelCommandResponse
import io.airbyte.api.model.generated.CheckCommandOutputRequest
import io.airbyte.api.model.generated.CheckCommandOutputResponse
import io.airbyte.api.model.generated.CommandGetRequest
import io.airbyte.api.model.generated.CommandGetResponse
import io.airbyte.api.model.generated.CommandStatusRequest
import io.airbyte.api.model.generated.CommandStatusResponse
import io.airbyte.api.model.generated.DiscoverCommandOutputRequest
import io.airbyte.api.model.generated.DiscoverCommandOutputResponse
import io.airbyte.api.model.generated.ReplicateCommandOutputRequest
import io.airbyte.api.model.generated.ReplicateCommandOutputResponse
import io.airbyte.api.model.generated.RunCheckCommandRequest
import io.airbyte.api.model.generated.RunCheckCommandResponse
import io.airbyte.api.model.generated.RunDiscoverCommandRequest
import io.airbyte.api.model.generated.RunDiscoverCommandResponse
import io.airbyte.api.model.generated.RunReplicateCommandRequest
import io.airbyte.api.model.generated.RunReplicateCommandResponse
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.enums.toEnum
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.commons.server.helpers.SecretSanitizer
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.temporal.scheduling.ReplicationCommandApiInput
import io.airbyte.config.FailureReason
import io.airbyte.config.ReplicationOutput
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.config.WorkloadPriority
import io.airbyte.protocol.models.Jsons
import io.airbyte.server.helpers.CatalogDiffConverter.toDomain
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
  private val catalogConverter: CatalogConverter,
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
    val output = commandService.getCheckJobOutput(checkCommandOutputRequest.id)
    return CheckCommandOutputResponse().apply {
      id(checkCommandOutputRequest.id)
      output?.let {
        status(it.checkConnection?.status?.toApi())
        message(it.checkConnection?.message)
        failureReason(it.failureReason.toApi())
      }
    }
  }

  @Post("/get")
  @Secured(AuthRoleConstants.WORKSPACE_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getCommand(
    @Body commandGetRequest: CommandGetRequest,
  ): CommandGetResponse {
    val command = commandService.get(commandGetRequest.id)
    return CommandGetResponse().apply {
      id(commandGetRequest?.id)
      command?.let {
        commandType(it.commandType)
        commandInput(it.commandInput.toString())
        workspaceId(it.workspaceId)
        workloadId(it.workloadId)
        organizationId(it.organizationId)
        createdAt(it.createdAt)
        updatedAt(it.updatedAt)
      }
    }
  }

  @Post("/output/discover")
  @Secured(AuthRoleConstants.WORKSPACE_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getDiscoverCommandOutput(
    @Body discoverCommandOutputRequest: DiscoverCommandOutputRequest,
  ): DiscoverCommandOutputResponse {
    val output = commandService.getDiscoverJobOutput(discoverCommandOutputRequest.id)
    // TODO the domain catalog to api catalog should be simpler however, the existing converter does all this...
    val apiCatalog =
      output?.catalog?.let {
        val protocolCatalog = Jsons.`object`(it.catalog, io.airbyte.protocol.models.v0.AirbyteCatalog::class.java)
        catalogConverter.toApi(protocolCatalog, null)
      }
    return DiscoverCommandOutputResponse().apply {
      id(discoverCommandOutputRequest.id)
      output?.let {
        status(if (it.failureReason == null) DiscoverCommandOutputResponse.StatusEnum.SUCCEEDED else DiscoverCommandOutputResponse.StatusEnum.FAILED)
        catalogId(output.catalogId)
        catalog(apiCatalog)
        failureReason(it.failureReason.toApi())
      }
    }
  }

  @Post("/output/replicate")
  @Secured(AuthRoleConstants.WORKSPACE_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getReplicateCommandOutput(
    @Body replicateCommandOutputRequest: ReplicateCommandOutputRequest,
  ): ReplicateCommandOutputResponse {
    val output: ReplicationOutput? = commandService.getReplicationOutput(replicateCommandOutputRequest.id)
    return ReplicateCommandOutputResponse().apply {
      id(replicateCommandOutputRequest.id)
      attemptSummary(output?.replicationAttemptSummary)
      failures(output?.failures?.map { it.toApi() })
      catalog(output?.outputCatalog?.let { catalogConverter.toApi(it, null) })
    }
  }

  private fun FailureReason?.toApi(): ApiFailureReason? =
    when (this) {
      null -> null
      else ->
        ApiFailureReason()
          .failureOrigin(this.failureOrigin?.convertTo())
          .failureType(this.failureType?.convertTo())
          .externalMessage(this.externalMessage)
          .internalMessage(this.internalMessage)
          .stacktrace(this.stacktrace)
          .timestamp(this.timestamp)
          .retryable(this.retryable)
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
    val priority: WorkloadPriority = runCheckCommandRequest.priority?.toWorkloadPriority() ?: WorkloadPriority.DEFAULT
    if (runCheckCommandRequest.actorId != null) {
      commandService.createCheckCommand(
        commandId = runCheckCommandRequest.id,
        actorId = runCheckCommandRequest.actorId,
        jobId = runCheckCommandRequest.jobId,
        attemptNumber = runCheckCommandRequest.attemptNumber?.toLong(),
        workloadPriority = priority,
        signalInput = runCheckCommandRequest.signalInput,
        commandInput = Jsons.jsonNode(runCheckCommandRequest),
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
        commandInput = Jsons.jsonNode(runCheckCommandRequest),
      )
    }
    return RunCheckCommandResponse().id(runCheckCommandRequest.id)
  }

  @Post("/run/discover")
  @Secured(AuthRoleConstants.WORKSPACE_RUNNER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun runDiscoverCommand(runDiscoverCommandRequest: RunDiscoverCommandRequest): RunDiscoverCommandResponse {
    val priority: WorkloadPriority = runDiscoverCommandRequest.priority?.toWorkloadPriority() ?: WorkloadPriority.DEFAULT
    commandService.createDiscoverCommand(
      commandId = runDiscoverCommandRequest.id,
      actorId = runDiscoverCommandRequest.actorId,
      jobId = runDiscoverCommandRequest.jobId,
      attemptNumber = runDiscoverCommandRequest.attemptNumber?.toLong(),
      workloadPriority = priority,
      signalInput = runDiscoverCommandRequest.signalInput,
      commandInput = Jsons.jsonNode(runDiscoverCommandRequest),
    )
    return RunDiscoverCommandResponse().id(runDiscoverCommandRequest.id)
  }

  @Post("/run/replicate")
  @Secured(AuthRoleConstants.WORKSPACE_RUNNER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun runReplicateCommand(runReplicateCommandRequest: RunReplicateCommandRequest): RunReplicateCommandResponse {
    commandService.createReplicateCommand(
      commandId = runReplicateCommandRequest.id,
      connectionId = runReplicateCommandRequest.connectionId,
      appliedCatalogDiff = runReplicateCommandRequest.appliedCatalogDiff?.toDomain(),
      jobId = runReplicateCommandRequest.jobId,
      attemptNumber = runReplicateCommandRequest.attemptNumber.toLong(),
      signalInput = runReplicateCommandRequest.signalInput,
      commandInput =
        Jsons.jsonNode(
          ReplicationCommandApiInput.ReplicationApiInput(
            connectionId = runReplicateCommandRequest.connectionId,
            jobId = runReplicateCommandRequest.jobId,
            attemptId = runReplicateCommandRequest.attemptNumber.toLong(),
            appliedCatalogDiff = runReplicateCommandRequest.appliedCatalogDiff?.toDomain(),
          ),
        ),
    )
    return RunReplicateCommandResponse().id(runReplicateCommandRequest.id)
  }

  private fun String.toWorkloadPriority(): WorkloadPriority? = uppercase().toEnum<WorkloadPriority>()
}
