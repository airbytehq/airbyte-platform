/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
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
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.enums.toEnum
import io.airbyte.commons.logging.LogUtils
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.converters.JobConverter
import io.airbyte.commons.server.converters.toApi
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.temporal.scheduling.ReplicationCommandApiInput
import io.airbyte.config.ReplicationOutput
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.config.WorkloadPriority
import io.airbyte.config.toModel
import io.airbyte.data.repositories.ActorRepository
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.models.ActorId
import io.airbyte.domain.models.CommandId
import io.airbyte.domain.models.ConnectionId
import io.airbyte.domain.models.WorkspaceId
import io.airbyte.protocol.models.Jsons
import io.airbyte.server.apis.requireBody
import io.airbyte.server.auth.CheckCommandRule
import io.airbyte.server.helpers.CatalogDiffConverter.toDomain
import io.airbyte.server.services.CommandService
import io.micronaut.context.annotation.Context
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.util.UUID
import io.airbyte.api.server.generated.models.JobLogs as ApiJobLogs
import io.airbyte.commons.json.Jsons as CommonsJsons

@Controller
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
class CommandApiController(
  private val roleResolver: RoleResolver,
  private val actorRepository: ActorRepository,
  private val catalogConverter: CatalogConverter,
  private val apiPojoConverters: ApiPojoConverters,
  private val commandService: CommandService,
  private val workspaceService: WorkspaceService,
  private val logUtils: LogUtils,
) : CommandApi {
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun cancelCommand(
    @Body cancelCommandRequestBody: CancelCommandRequest?,
  ): CancelCommandResponse {
    val cancelCommandRequest = cancelCommandRequestBody.requireBody()
    return withRoleValidation(
      id = CommandId(cancelCommandRequest.id),
      role = AuthRoleConstants.WORKSPACE_RUNNER,
    ) {
      commandService.cancel(cancelCommandRequest.id)
      return CancelCommandResponse(id = cancelCommandRequest.id)
    }
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getCheckCommandOutput(
    @Body checkCommandOutputRequestBody: CheckCommandOutputRequest?,
  ): CheckCommandOutputResponse {
    val checkCommandOutputRequest = checkCommandOutputRequestBody.requireBody()
    return withRoleValidation(
      id = CommandId(checkCommandOutputRequest.id),
      roles = setOf(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.EMBEDDED_END_USER),
    ) {
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
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getCommand(
    @Body commandGetRequestBody: CommandGetRequest?,
  ): CommandGetResponse {
    val commandGetRequest = commandGetRequestBody.requireBody()
    return withRoleValidation(
      id = CommandId(commandGetRequest.id),
      role = AuthRoleConstants.WORKSPACE_READER,
    ) {
      // withRoleValidation already rejected a missing command
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
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getDiscoverCommandOutput(
    @Body discoverCommandOutputRequestBody: DiscoverCommandOutputRequest?,
  ): DiscoverCommandOutputResponse {
    val discoverCommandOutputRequest = discoverCommandOutputRequestBody.requireBody()
    return withRoleValidation(
      id = CommandId(discoverCommandOutputRequest.id),
      role = AuthRoleConstants.WORKSPACE_READER,
    ) {
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
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getSpecCommandOutput(
    @Body specCommandOutputRequestBody: SpecCommandOutputRequest?,
  ): SpecCommandOutputResponse {
    val specCommandOutputRequest = specCommandOutputRequestBody.requireBody()
    return withRoleValidation(
      id = CommandId(specCommandOutputRequest.id),
      role = AuthRoleConstants.WORKSPACE_READER,
    ) {
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
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getReplicateCommandOutput(
    @Body replicateCommandOutputRequestBody: ReplicateCommandOutputRequest?,
  ): ReplicateCommandOutputResponse {
    val replicateCommandOutputRequest = replicateCommandOutputRequestBody.requireBody()
    return withRoleValidation(
      id = CommandId(replicateCommandOutputRequest.id),
      role = AuthRoleConstants.WORKSPACE_READER,
    ) {
      val output: ReplicationOutput? = commandService.getReplicationOutput(replicateCommandOutputRequest.id)
      return ReplicateCommandOutputResponse(
        id = replicateCommandOutputRequest.id,
        attemptSummary = output?.replicationAttemptSummary,
        failures = output?.failures?.mapNotNull { apiPojoConverters.failureReasonToApi(it) },
      )
    }
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
    migration = RouteMigration.LEGACY,
  )
  override fun getCommandStatus(
    @Body commandStatusRequestBody: CommandStatusRequest?,
  ): CommandStatusResponse {
    val commandStatusRequest = commandStatusRequestBody.requireBody()
    return withRoleValidation(
      id = CommandId(commandStatusRequest.id),
      role = AuthRoleConstants.WORKSPACE_READER,
    ) {
      val commandStatus = commandService.getStatus(commandStatusRequest.id)
      return CommandStatusResponse(
        id = commandStatusRequest.id,
        status = commandStatus?.convertTo<CommandStatusResponse.Status>(),
      )
    }
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
    migration = RouteMigration.LEGACY,
  )
  override fun runDiscoverCommand(
    @Body runDiscoverCommandRequestBody: RunDiscoverCommandRequest?,
  ): RunDiscoverCommandResponse {
    val runDiscoverCommandRequest = runDiscoverCommandRequestBody.requireBody()
    return withRoleValidation(
      id = ActorId(runDiscoverCommandRequest.actorId),
      role = AuthRoleConstants.WORKSPACE_RUNNER,
    ) {
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
  }

  @Secured(AuthRoleConstants.WORKSPACE_RUNNER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun runReplicateCommand(
    @Body runReplicateCommandRequestBody: RunReplicateCommandRequest?,
  ): RunReplicateCommandResponse {
    val runReplicateCommandRequest = runReplicateCommandRequestBody.requireBody()
    return withRoleValidation(
      id = ConnectionId(runReplicateCommandRequest.connectionId),
      role = AuthRoleConstants.WORKSPACE_RUNNER,
    ) {
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
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun runSpecCommand(
    @Body runSpecCommandRequestBody: RunSpecCommandRequest?,
  ): RunSpecCommandResponse {
    val runSpecCommandRequest = runSpecCommandRequestBody.requireBody()
    return withRoleValidation(
      id = WorkspaceId(runSpecCommandRequest.workspaceId),
      role = AuthRoleConstants.WORKSPACE_RUNNER,
    ) {
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
  }

  @InternalForTesting
  internal final inline fun <reified IdType> resolveWorkspaceId(id: IdType): UUID =
    when (id) {
      is ActorId -> actorRepository.findByActorId(id.value)?.workspaceId
      is CommandId -> commandService.get(commandId = id.value)?.workspaceId
      is ConnectionId -> workspaceService.getStandardWorkspaceFromConnection(id.value, false).workspaceId
      is WorkspaceId -> id.value
      else -> throw IllegalStateException("Unsupported id $id of type ${IdType::class.simpleName}")
    } ?: throw ForbiddenProblem(ProblemMessageData().message("User does not have the required permissions to access the resource(s)."))

  @InternalForTesting
  internal final inline fun <reified IdType, T> withRoleValidation(
    id: IdType,
    role: String,
    call: () -> T,
  ): T {
    val workspaceId = resolveWorkspaceId(id)

    roleResolver
      .newRequest()
      .withCurrentAuthentication()
      .withRef(AuthenticationId.WORKSPACE_ID, workspaceId)
      .requireOneOfRoles(setOf(role))

    return call()
  }

  @InternalForTesting
  internal final inline fun <reified IdType, T> withRoleValidation(
    id: IdType,
    roles: Set<String>,
    call: () -> T,
  ): T {
    val workspaceId = resolveWorkspaceId(id)

    roleResolver
      .newRequest()
      .withCurrentAuthentication()
      .withRef(AuthenticationId.WORKSPACE_ID, workspaceId)
      .requireOneOfRoles(roles)

    return call()
  }

  private fun String.toWorkloadPriority(): WorkloadPriority? = uppercase().toEnum<WorkloadPriority>()
}
