/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.CommandApi
import io.airbyte.api.server.generated.models.CancelCommandRequest
import io.airbyte.api.server.generated.models.CancelCommandResponse
import io.airbyte.api.server.generated.models.CheckCommandOutputRequest
import io.airbyte.api.server.generated.models.CheckCommandOutputResponse
import io.airbyte.api.server.generated.models.CommandGetRequest
import io.airbyte.api.server.generated.models.CommandGetResponse
import io.airbyte.api.server.generated.models.CommandStatusRequest
import io.airbyte.api.server.generated.models.CommandStatusResponse
import io.airbyte.api.server.generated.models.DiscoverCommandOutputRequest
import io.airbyte.api.server.generated.models.DiscoverCommandOutputResponse
import io.airbyte.api.server.generated.models.LogEvents
import io.airbyte.api.server.generated.models.LogFormatType
import io.airbyte.api.server.generated.models.ReplicateCommandOutputRequest
import io.airbyte.api.server.generated.models.ReplicateCommandOutputResponse
import io.airbyte.api.server.generated.models.RunCheckCommandRequest
import io.airbyte.api.server.generated.models.RunCheckCommandResponse
import io.airbyte.api.server.generated.models.RunDiscoverCommandRequest
import io.airbyte.api.server.generated.models.RunDiscoverCommandResponse
import io.airbyte.api.server.generated.models.RunReplicateCommandRequest
import io.airbyte.api.server.generated.models.RunReplicateCommandResponse
import io.airbyte.api.server.generated.models.RunSpecCommandRequest
import io.airbyte.api.server.generated.models.RunSpecCommandResponse
import io.airbyte.api.server.generated.models.SpecCommandOutputRequest
import io.airbyte.api.server.generated.models.SpecCommandOutputResponse
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.enums.toEnum
import io.airbyte.commons.logging.LogUtils
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.converters.JobConverter
import io.airbyte.commons.server.converters.toApi
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.temporal.scheduling.ReplicationCommandApiInput
import io.airbyte.config.ReplicationOutput
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.config.WorkloadPriority
import io.airbyte.config.toModel
import io.airbyte.protocol.models.Jsons
import io.airbyte.server.apis.requireBody
import io.airbyte.server.auth.CancelCommandRule
import io.airbyte.server.auth.CheckCommandRule
import io.airbyte.server.auth.ReadCheckCommandRule
import io.airbyte.server.auth.ReadCommandRule
import io.airbyte.server.auth.RunDiscoverCommandRule
import io.airbyte.server.auth.RunReplicateCommandRule
import io.airbyte.server.auth.RunSpecCommandRule
import io.airbyte.server.helpers.CatalogDiffConverter.toDomain
import io.airbyte.server.services.CommandService
import io.micronaut.context.annotation.Context
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import io.airbyte.api.server.generated.models.JobLogs as ApiJobLogs
import io.airbyte.commons.json.Jsons as CommonsJsons

@Controller
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
class CommandApiController(
  private val catalogConverter: CatalogConverter,
  private val apiPojoConverters: ApiPojoConverters,
  private val commandService: CommandService,
  private val logUtils: LogUtils,
) : CommandApi {
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = CancelCommandRule::class,
  )
  override fun cancelCommand(
    @Body cancelCommandRequestBody: CancelCommandRequest?,
  ): CancelCommandResponse {
    val cancelCommandRequest = cancelCommandRequestBody.requireBody()
    commandService.cancel(cancelCommandRequest.id)
    return CancelCommandResponse(id = cancelCommandRequest.id)
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = ReadCheckCommandRule::class,
  )
  override fun getCheckCommandOutput(
    @Body checkCommandOutputRequestBody: CheckCommandOutputRequest?,
  ): CheckCommandOutputResponse {
    val checkCommandOutputRequest = checkCommandOutputRequestBody.requireBody()
    val output =
      commandService.getCheckJobOutput(
        commandId = checkCommandOutputRequest.id,
        withLogs = checkCommandOutputRequest.withLogs ?: false,
      )
    // the output is absent while the command is still running
    return CheckCommandOutputResponse(
      id = checkCommandOutputRequest.id,
      status = output?.status?.toApi(),
      connectorConfigurationUpdated = output?.connectorConfigUpdated,
      message = output?.message,
      failureReason = output?.let { apiPojoConverters.failureReasonToApi(it.failureReason) },
      logs = output?.logs?.toApi(),
    )
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = ReadCommandRule::class,
  )
  override fun getCommand(
    @Body commandGetRequestBody: CommandGetRequest?,
  ): CommandGetResponse {
    val commandGetRequest = commandGetRequestBody.requireBody()
    val command = requireNotNull(commandService.get(commandGetRequest.id)) { "Command ${commandGetRequest.id} not found" }
    return CommandGetResponse(
      id = commandGetRequest.id,
      commandType = command.commandType,
      commandInput = command.commandInput.toString(),
      workspaceId = command.workspaceId,
      workloadId = command.workloadId,
      organizationId = command.organizationId,
      createdAt = command.createdAt,
      updatedAt = command.updatedAt,
    )
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = ReadCommandRule::class,
  )
  override fun getDiscoverCommandOutput(
    @Body discoverCommandOutputRequestBody: DiscoverCommandOutputRequest?,
  ): DiscoverCommandOutputResponse {
    val discoverCommandOutputRequest = discoverCommandOutputRequestBody.requireBody()
    val output = commandService.getDiscoverJobOutput(discoverCommandOutputRequest.id, withLogs = discoverCommandOutputRequest.withLogs ?: false)
    // TODO the domain catalog to api catalog should be simpler however, the existing converter does all this...
    val apiCatalog =
      output?.catalog?.let {
        val protocolCatalog = Jsons.`object`(it.catalog, io.airbyte.protocol.models.v0.AirbyteCatalog::class.java)
        catalogConverter.toApi(protocolCatalog, null)
      }
    val apiDestinationCatalog =
      output?.destinationCatalog?.let {
        val protocolDestinationCatalog = Jsons.`object`(it.catalog, io.airbyte.protocol.models.v0.DestinationCatalog::class.java)
        protocolDestinationCatalog.toModel().toApi()
      }
    // the output is absent while the command is still running
    return DiscoverCommandOutputResponse(
      id = discoverCommandOutputRequest.id,
      status =
        output?.let {
          if (it.failureReason == null) DiscoverCommandOutputResponse.Status.SUCCEEDED else DiscoverCommandOutputResponse.Status.FAILED
        },
      catalogId = output?.catalogId,
      catalog = apiCatalog,
      destinationCatalog = apiDestinationCatalog,
      failureReason = output?.let { apiPojoConverters.failureReasonToApi(it.failureReason) },
      logs = output?.logs?.toApi(),
    )
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = ReadCommandRule::class,
  )
  override fun getSpecCommandOutput(
    @Body specCommandOutputRequestBody: SpecCommandOutputRequest?,
  ): SpecCommandOutputResponse {
    val specCommandOutputRequest = specCommandOutputRequestBody.requireBody()
    val output = commandService.getSpecJobOutput(specCommandOutputRequest.id, withLogs = specCommandOutputRequest.withLogs ?: false)
    // the output is absent while the command is still running
    return SpecCommandOutputResponse(
      id = specCommandOutputRequest.id,
      status =
        output?.let {
          if (it.failureReason == null) SpecCommandOutputResponse.Status.SUCCEEDED else SpecCommandOutputResponse.Status.FAILED
        },
      spec = output?.spec?.let { spec -> Jsons.jsonNode(spec) },
      failureReason = output?.let { apiPojoConverters.failureReasonToApi(it.failureReason) },
      logs = output?.logs?.toApi(),
    )
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = ReadCommandRule::class,
  )
  override fun getReplicateCommandOutput(
    @Body replicateCommandOutputRequestBody: ReplicateCommandOutputRequest?,
  ): ReplicateCommandOutputResponse {
    val replicateCommandOutputRequest = replicateCommandOutputRequestBody.requireBody()
    val output: ReplicationOutput? = commandService.getReplicationOutput(replicateCommandOutputRequest.id)
    return ReplicateCommandOutputResponse(
      id = replicateCommandOutputRequest.id,
      attemptSummary = output?.replicationAttemptSummary,
      failures = output?.failures?.mapNotNull { apiPojoConverters.failureReasonToApi(it) },
    )
  }

  private fun StandardCheckConnectionOutput.Status.toApi(): CheckCommandOutputResponse.Status =
    when (this) {
      StandardCheckConnectionOutput.Status.SUCCEEDED -> CheckCommandOutputResponse.Status.SUCCEEDED
      StandardCheckConnectionOutput.Status.FAILED -> CheckCommandOutputResponse.Status.FAILED
    }

  private fun CommandService.JobLogs.toApi(): ApiJobLogs =
    if (isStructured()) {
      ApiJobLogs(
        logType = LogFormatType.STRUCTURED,
        logEvents =
          LogEvents(
            events = logEvents?.events?.let { JobConverter.toModelLogEvents(it, logUtils) }.orEmpty(),
            version = logEvents?.version,
          ),
      )
    } else {
      ApiJobLogs(logType = LogFormatType.FORMATTED, logLines = logLines)
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = ReadCommandRule::class,
  )
  override fun getCommandStatus(
    @Body commandStatusRequestBody: CommandStatusRequest?,
  ): CommandStatusResponse {
    val commandStatusRequest = commandStatusRequestBody.requireBody()
    val commandStatus = commandService.getStatus(commandStatusRequest.id)
    return CommandStatusResponse(
      id = commandStatusRequest.id,
      status = commandStatus?.convertTo<CommandStatusResponse.Status>(),
    )
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = CheckCommandRule::class,
  )
  override fun runCheckCommand(
    @Body runCheckCommandRequestBody: RunCheckCommandRequest?,
  ): RunCheckCommandResponse {
    val runCheckCommandRequest = runCheckCommandRequestBody.requireBody()
    val priority: WorkloadPriority = runCheckCommandRequest.priority?.toWorkloadPriority() ?: WorkloadPriority.DEFAULT

    // Process and sanitize config if present
    val sanitizedConfig =
      if (runCheckCommandRequest.config != null) {
        // Get actor context from service
        val context =
          commandService.getCheckCommandContext(
            actorId = runCheckCommandRequest.actorId,
            actorDefinitionId = runCheckCommandRequest.actorDefinitionId,
            workspaceId = runCheckCommandRequest.workspaceId,
          )

        commandService.processCheckConfig(
          actorId = runCheckCommandRequest.actorId,
          actorDefinitionId = context.actorDefinitionId,
          workspaceId = context.workspaceId,
          providedConfig = Jsons.jsonNode(runCheckCommandRequest.config),
        )
      } else {
        null
      }

    // Record the request with the sanitized config as commandInput (logging/auditing)
    val commandInputRequest = if (sanitizedConfig != null) runCheckCommandRequest.copy(config = sanitizedConfig) else runCheckCommandRequest

    commandService.createCheckCommand(
      commandId = runCheckCommandRequest.id,
      actorId = runCheckCommandRequest.actorId,
      actorDefinitionId = runCheckCommandRequest.actorDefinitionId,
      workspaceId = runCheckCommandRequest.workspaceId,
      configuration = sanitizedConfig,
      jobId = runCheckCommandRequest.jobId,
      attemptNumber = runCheckCommandRequest.attemptNumber?.toLong(),
      workloadPriority = priority,
      signalInput = runCheckCommandRequest.signalInput,
      commandInput = CommonsJsons.jsonNode(commandInputRequest),
    )

    return RunCheckCommandResponse(id = runCheckCommandRequest.id)
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = RunDiscoverCommandRule::class,
  )
  override fun runDiscoverCommand(
    @Body runDiscoverCommandRequestBody: RunDiscoverCommandRequest?,
  ): RunDiscoverCommandResponse {
    val runDiscoverCommandRequest = runDiscoverCommandRequestBody.requireBody()
    val priority: WorkloadPriority = runDiscoverCommandRequest.priority?.toWorkloadPriority() ?: WorkloadPriority.DEFAULT
    commandService.createDiscoverCommand(
      commandId = runDiscoverCommandRequest.id,
      actorId = runDiscoverCommandRequest.actorId,
      jobId = runDiscoverCommandRequest.jobId,
      attemptNumber = runDiscoverCommandRequest.attemptNumber?.toLong(),
      workloadPriority = priority,
      signalInput = runDiscoverCommandRequest.signalInput,
      commandInput = CommonsJsons.jsonNode(runDiscoverCommandRequest),
    )
    return RunDiscoverCommandResponse(id = runDiscoverCommandRequest.id)
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = RunReplicateCommandRule::class,
  )
  override fun runReplicateCommand(
    @Body runReplicateCommandRequestBody: RunReplicateCommandRequest?,
  ): RunReplicateCommandResponse {
    val runReplicateCommandRequest = runReplicateCommandRequestBody.requireBody()
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
    return RunReplicateCommandResponse(id = runReplicateCommandRequest.id)
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = RunSpecCommandRule::class,
  )
  override fun runSpecCommand(
    @Body runSpecCommandRequestBody: RunSpecCommandRequest?,
  ): RunSpecCommandResponse {
    val runSpecCommandRequest = runSpecCommandRequestBody.requireBody()
    val actorDefinitionId = runSpecCommandRequest.actorDefinitionId
    if (actorDefinitionId != null) {
      commandService.createSpecCommand(
        commandId = runSpecCommandRequest.id,
        actorDefinitionId = actorDefinitionId,
        dockerImageTag = runSpecCommandRequest.dockerImageTag,
        workspaceId = runSpecCommandRequest.workspaceId,
        signalInput = runSpecCommandRequest.signalInput,
        commandInput = CommonsJsons.jsonNode(runSpecCommandRequest),
      )
    } else {
      commandService.createSpecCommand(
        commandId = runSpecCommandRequest.id,
        dockerImage =
          runSpecCommandRequest.dockerImage ?: throw IllegalArgumentException("docker_image is required when actor_definition_id is not provided"),
        dockerImageTag = runSpecCommandRequest.dockerImageTag,
        workspaceId = runSpecCommandRequest.workspaceId,
        signalInput = runSpecCommandRequest.signalInput,
        commandInput = CommonsJsons.jsonNode(runSpecCommandRequest),
      )
    }
    return RunSpecCommandResponse(id = runSpecCommandRequest.id)
  }

  private fun String.toWorkloadPriority(): WorkloadPriority? = uppercase().toEnum<WorkloadPriority>()
}
