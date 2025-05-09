/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.logging.LogClientManager
import io.airbyte.commons.temporal.TemporalUtils
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.WorkloadPriority
import io.airbyte.config.WorkloadType
import io.airbyte.data.repositories.ActorRepository
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.server.repositories.CommandsRepository
import io.airbyte.server.repositories.domain.Command
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workload.common.WorkloadLabels
import io.airbyte.workload.common.WorkloadQueueService
import io.airbyte.workload.output.WorkloadOutputDocStoreReader
import io.airbyte.workload.repository.domain.WorkloadLabel
import io.airbyte.workload.repository.domain.WorkloadStatus
import io.airbyte.workload.services.NotFoundException
import io.airbyte.workload.services.WorkloadService
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.nio.file.Path
import java.time.OffsetDateTime
import java.util.UUID

enum class CommandStatus {
  PENDING,
  RUNNING,
  COMPLETED,
  CANCELLED,
}

enum class CommandType {
  CHECK,
  DISCOVER,
  REPLICATE,
}

private data class WorkloadCreatePayload(
  val workloadId: String,
  val labels: List<WorkloadLabel>,
  val workloadInput: String,
  val logPath: String,
  val type: WorkloadType,
  val priority: WorkloadPriority,
  val dataplaneGroupId: UUID,
  val signalInput: String?,
)

@Singleton
class CommandService(
  private val actorRepository: ActorRepository,
  private val commandsRepository: CommandsRepository,
  private val jobInputService: JobInputService,
  private val logClientManager: LogClientManager,
  private val organizationService: OrganizationService,
  private val workloadService: WorkloadService,
  private val workloadQueueService: WorkloadQueueService,
  private val workloadOutputReader: WorkloadOutputDocStoreReader,
  private val workspaceService: WorkspaceService,
  @Named("workspaceRoot") private val workspaceRoot: Path,
) {
  fun createCheckCommand(
    commandId: String,
    actorDefinitionId: UUID,
    workspaceId: UUID,
    configuration: JsonNode,
    workloadPriority: WorkloadPriority,
    signalInput: String?,
    commandInput: Any,
  ) {
    val checkInput = jobInputService.getCheckInput(actorDefinitionId, workspaceId, configuration)
    val workloadPayload = createCheckCreateWorkloadRequest(actorDefinitionId, workspaceId, checkInput, workloadPriority, signalInput)
    createCommand(
      commandId = commandId,
      commandType = CommandType.CHECK.name,
      commandInput = Jsons.jsonNode(commandInput),
      workspaceId = workspaceId,
      workloadPayload = workloadPayload,
    )
  }

  fun createCheckCommand(
    commandId: String,
    actorId: UUID,
    jobId: String?,
    attemptNumber: Long?,
    workloadPriority: WorkloadPriority,
    signalInput: String?,
    commandInput: Any,
  ) {
    val checkInput = jobInputService.getCheckInput(actorId, jobId, attemptNumber)
    val actor = actorRepository.findByActorId(actorId) ?: throw NotFoundException("Unable to find actorId $actorId")
    val workspaceId = actor.workspaceId
    val workloadPayload =
      createCheckCreateWorkloadRequest(
        actorDefinitionId = actor.actorDefinitionId,
        workspaceId = workspaceId,
        checkInput = checkInput,
        workloadPriority = workloadPriority,
        signalInput = signalInput,
      )
    createCommand(
      commandId = commandId,
      commandType = CommandType.CHECK.name,
      commandInput = Jsons.jsonNode(commandInput),
      workspaceId = workspaceId,
      workloadPayload = workloadPayload,
    )
  }

  private fun createCheckCreateWorkloadRequest(
    actorDefinitionId: UUID,
    workspaceId: UUID,
    checkInput: CheckConnectionInput,
    workloadPriority: WorkloadPriority,
    signalInput: String?,
  ): WorkloadCreatePayload {
    val jobId = checkInput.jobRunConfig.jobId
    val attemptNumber = checkInput.jobRunConfig.attemptId // because this is the only place where it's attemptId

    // TODO this should come from WorkloadIdGenerator
    val workloadId = "${actorDefinitionId}_${jobId}_${attemptNumber}_check"
    val labels =
      listOfNotNull(
        WorkloadLabel(key = WorkloadLabels.JOB_LABEL_KEY, value = jobId),
        WorkloadLabel(key = WorkloadLabels.ATTEMPT_LABEL_KEY, value = attemptNumber.toString()),
        WorkloadLabel(key = WorkloadLabels.WORKSPACE_LABEL_KEY, value = workspaceId.toString()),
        WorkloadLabel(key = WorkloadLabels.ACTOR_TYPE, value = checkInput.checkConnectionInput.actorType.toString()),
      )

    val dataplaneGroupId = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false).dataplaneGroupId

    return WorkloadCreatePayload(
      workloadId = workloadId,
      labels = labels,
      workloadInput = Jsons.serialize(checkInput),
      logPath = logClientManager.fullLogPath(TemporalUtils.getJobRoot(workspaceRoot, jobId, attemptNumber)),
      type = WorkloadType.CHECK,
      priority = workloadPriority,
      dataplaneGroupId = dataplaneGroupId,
      signalInput = signalInput,
    )
  }

  private fun createCommand(
    commandId: String,
    commandType: String,
    commandInput: JsonNode,
    workspaceId: UUID,
    workloadPayload: WorkloadCreatePayload,
  ) {
    val organizationId = organizationService.getOrganizationForWorkspaceId(workspaceId).map { it.organizationId }.orElseThrow()
    val currentTime = OffsetDateTime.now()
    val command =
      Command(
        id = commandId,
        workloadId = workloadPayload.workloadId,
        commandType = commandType,
        commandInput = commandInput,
        workspaceId = workspaceId,
        organizationId = organizationId,
        createdAt = currentTime,
        updatedAt = currentTime,
      )
    val mutexKey: String? = null
    val workloadAutoId = UUID.randomUUID()

    // TODO the workloadService.createWorkload and Command.save should be in a transaction
    workloadService.createWorkload(
      workloadId = workloadPayload.workloadId,
      labels = workloadPayload.labels,
      input = workloadPayload.workloadInput,
      logPath = workloadPayload.logPath,
      mutexKey = mutexKey,
      type = workloadPayload.type,
      autoId = workloadAutoId,
      deadline = null,
      signalInput = workloadPayload.signalInput,
      dataplaneGroup = workloadPayload.dataplaneGroupId.toString(),
      priority = workloadPayload.priority,
    )

    // because workloadId is a foreign key
    commandsRepository.save(command)

    // TODO this should only run if both saves above are successful
    workloadQueueService.create(
      workloadId = workloadPayload.workloadId,
      workloadInput = workloadPayload.workloadInput,
      labels = workloadPayload.labels.associate { it.key to it.value },
      logPath = workloadPayload.logPath,
      mutexKey = mutexKey,
      workloadType = workloadPayload.type,
      autoId = workloadAutoId,
      priority = workloadPayload.priority,
      dataplaneGroup = workloadPayload.dataplaneGroupId.toString(),
    )
  }

  fun cancel(commandId: String) {
    commandsRepository.findById(commandId).ifPresent { command ->
      workloadService.cancelWorkload(command.workloadId, "api", "cancelled from the api")
    }
  }

  fun getStatus(commandId: String): CommandStatus? =
    commandsRepository
      .findById(commandId)
      .map { command ->
        try {
          when (workloadService.getWorkload(command.workloadId).status) {
            WorkloadStatus.PENDING -> CommandStatus.PENDING
            WorkloadStatus.CLAIMED -> CommandStatus.PENDING
            WorkloadStatus.LAUNCHED -> CommandStatus.PENDING
            WorkloadStatus.RUNNING -> CommandStatus.RUNNING
            WorkloadStatus.SUCCESS -> CommandStatus.COMPLETED
            WorkloadStatus.FAILURE -> CommandStatus.COMPLETED
            WorkloadStatus.CANCELLED -> CommandStatus.CANCELLED
          }
        } catch (e: NotFoundException) {
          null
        }
      }.orElse(null)

  fun getConnectorJobOutput(commandId: String): ConnectorJobOutput? =
    commandsRepository
      .findById(commandId)
      .map { command ->
        workloadOutputReader.readConnectorOutput(command.workloadId)
      }.orElse(null)
}
