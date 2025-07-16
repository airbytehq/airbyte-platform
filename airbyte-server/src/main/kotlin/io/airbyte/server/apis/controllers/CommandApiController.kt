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
import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.converters.ApiConverters.Companion.toApi
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.enums.toEnum
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.commons.server.helpers.SecretSanitizer
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.temporal.scheduling.ReplicationCommandApiInput
import io.airbyte.config.FailureReason
import io.airbyte.config.ReplicationOutput
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.config.WorkloadPriority
import io.airbyte.data.repositories.ActorRepository
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.models.ActorId
import io.airbyte.domain.models.CommandId
import io.airbyte.domain.models.ConnectionId
import io.airbyte.domain.models.WorkspaceId
import io.airbyte.persistence.job.errorreporter.JobErrorReporter
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
  private val roleResolver: RoleResolver,
  private val actorRepository: ActorRepository,
  private val catalogConverter: CatalogConverter,
  private val commandService: CommandService,
  private val secretSanitizer: SecretSanitizer,
  private val workspaceService: WorkspaceService,
) : CommandApi {
  @Post("/cancel")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun cancelCommand(
    @Body cancelCommandRequest: CancelCommandRequest,
  ): CancelCommandResponse =
    withRoleValidation(
      id = CommandId(cancelCommandRequest.id),
      role = AuthRoleConstants.WORKSPACE_RUNNER,
    ) {
      commandService.cancel(cancelCommandRequest.id)
      return CancelCommandResponse().id(cancelCommandRequest.id)
    }

  @Post("/output/check")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getCheckCommandOutput(
    @Body checkCommandOutputRequest: CheckCommandOutputRequest,
  ): CheckCommandOutputResponse =
    withRoleValidation(
      id = CommandId(checkCommandOutputRequest.id),
      role = AuthRoleConstants.WORKSPACE_READER,
    ) {
      val output = commandService.getCheckJobOutput(checkCommandOutputRequest.id)
      return CheckCommandOutputResponse().apply {
        id(checkCommandOutputRequest.id)
        output?.let {
          status(it.checkConnection?.status?.toApi())
          message(it.checkConnection?.message)
          failureReason(toApi(it.failureReason))
        }
      }
    }

  @Post("/get")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getCommand(
    @Body commandGetRequest: CommandGetRequest,
  ): CommandGetResponse =
    withRoleValidation(
      id = CommandId(commandGetRequest.id),
      role = AuthRoleConstants.WORKSPACE_READER,
    ) {
      val command = commandService.get(commandGetRequest.id)
      return CommandGetResponse().apply {
        id(commandGetRequest.id)
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
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getDiscoverCommandOutput(
    @Body discoverCommandOutputRequest: DiscoverCommandOutputRequest,
  ): DiscoverCommandOutputResponse =
    withRoleValidation(
      id = CommandId(discoverCommandOutputRequest.id),
      role = AuthRoleConstants.WORKSPACE_READER,
    ) {
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
          status(
            if (it.failureReason ==
              null
            ) {
              DiscoverCommandOutputResponse.StatusEnum.SUCCEEDED
            } else {
              DiscoverCommandOutputResponse.StatusEnum.FAILED
            },
          )
          catalogId(output.catalogId)
          catalog(apiCatalog)
          failureReason(toApi(it.failureReason))
        }
      }
    }

  @Post("/output/replicate")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getReplicateCommandOutput(
    @Body replicateCommandOutputRequest: ReplicateCommandOutputRequest,
  ): ReplicateCommandOutputResponse =
    withRoleValidation(
      id = CommandId(replicateCommandOutputRequest.id),
      role = AuthRoleConstants.WORKSPACE_READER,
    ) {
      val output: ReplicationOutput? = commandService.getReplicationOutput(replicateCommandOutputRequest.id)
      return ReplicateCommandOutputResponse().apply {
        id(replicateCommandOutputRequest.id)
        attemptSummary(output?.replicationAttemptSummary)
        failures(output?.failures?.map { toApi(it) })
        catalog(output?.outputCatalog?.let { catalogConverter.toApi(it, null) })
      }
    }

  internal fun toApi(failureReason: FailureReason?): ApiFailureReason? =
    when (failureReason) {
      null -> null
      else ->
        ApiFailureReason()
          .failureOrigin(failureReason.failureOrigin?.convertTo())
          .failureType(failureReason.failureType?.convertTo())
          .externalMessage(failureReason.externalMessage)
          .internalMessage(failureReason.internalMessage)
          .stacktrace(failureReason.stacktrace)
          .timestamp(failureReason.timestamp)
          .retryable(failureReason.retryable)
          .apply {
            failureReason.metadata?.let {
              fromTraceMessage = failureReason.metadata.additionalProperties[JobErrorReporter.FROM_TRACE_MESSAGE] as Boolean?
            }
            failureReason.streamDescriptor?.let { internalStreamDescriptor ->
              streamDescriptor = internalStreamDescriptor.toApi()
            }
          }
    }

  private fun StandardCheckConnectionOutput.Status.toApi(): CheckCommandOutputResponse.StatusEnum =
    when (this) {
      StandardCheckConnectionOutput.Status.SUCCEEDED -> CheckCommandOutputResponse.StatusEnum.SUCCEEDED
      StandardCheckConnectionOutput.Status.FAILED -> CheckCommandOutputResponse.StatusEnum.FAILED
    }

  @Post("/status")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getCommandStatus(
    @Body commandStatusRequest: CommandStatusRequest,
  ): CommandStatusResponse =
    withRoleValidation(
      id = CommandId(commandStatusRequest.id),
      role = AuthRoleConstants.WORKSPACE_READER,
    ) {
      val commandStatus = commandService.getStatus(commandStatusRequest.id)
      return CommandStatusResponse().apply {
        id(commandStatusRequest.id)
        commandStatus?.let { status(it.convertTo()) }
      }
    }

  @Post("/run/check")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun runCheckCommand(
    @Body runCheckCommandRequest: RunCheckCommandRequest,
  ): RunCheckCommandResponse =
    withRoleValidation(
      id =
        if (runCheckCommandRequest.workspaceId !=
          null
        ) {
          WorkspaceId(runCheckCommandRequest.workspaceId)
        } else {
          ActorId(runCheckCommandRequest.actorId)
        },
      role = AuthRoleConstants.WORKSPACE_RUNNER,
    ) {
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
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun runDiscoverCommand(runDiscoverCommandRequest: RunDiscoverCommandRequest): RunDiscoverCommandResponse =
    withRoleValidation(
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
        commandInput = Jsons.jsonNode(runDiscoverCommandRequest),
      )
      return RunDiscoverCommandResponse().id(runDiscoverCommandRequest.id)
    }

  @Post("/run/replicate")
  @Secured(AuthRoleConstants.WORKSPACE_RUNNER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun runReplicateCommand(runReplicateCommandRequest: RunReplicateCommandRequest): RunReplicateCommandResponse =
    withRoleValidation(
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
      return RunReplicateCommandResponse().id(runReplicateCommandRequest.id)
    }

  @InternalForTesting
  internal inline fun <reified IdType, T> withRoleValidation(
    id: IdType,
    role: String,
    call: () -> T,
  ): T {
    val workspaceId =
      when (id) {
        is ActorId -> actorRepository.findByActorId(id.value)?.workspaceId
        is CommandId -> commandService.get(commandId = id.value)?.workspaceId
        is ConnectionId -> workspaceService.getStandardWorkspaceFromConnection(id.value, false)?.workspaceId
        is WorkspaceId -> id.value
        else -> throw IllegalStateException("Unsupported id $id of type ${IdType::class.simpleName}")
      } ?: throw ForbiddenProblem(ProblemMessageData().message("User does not have the required $role permissions to access the resource(s)."))

    roleResolver
      .newRequest()
      .withCurrentAuthentication()
      .withRef(AuthenticationId.WORKSPACE_ID, workspaceId)
      .requireRole(role)

    return call()
  }

  private fun String.toWorkloadPriority(): WorkloadPriority? = uppercase().toEnum<WorkloadPriority>()
}
