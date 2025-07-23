/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.logging.LogClientManager
import io.airbyte.commons.temporal.TemporalUtils
import io.airbyte.commons.temporal.scheduling.DiscoverCommandInput
import io.airbyte.config.ActorCatalog
import io.airbyte.config.ActorType
import io.airbyte.config.CatalogDiff
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.ReplicationOutput
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.config.WorkloadPriority
import io.airbyte.config.WorkloadType
import io.airbyte.data.repositories.ActorRepository
import io.airbyte.data.services.CatalogService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.server.helpers.WorkloadIdGenerator
import io.airbyte.server.repositories.CommandsRepository
import io.airbyte.server.repositories.domain.Command
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workload.common.WorkloadLabels
import io.airbyte.workload.common.WorkloadQueueService
import io.airbyte.workload.output.WorkloadOutputDocStoreReader
import io.airbyte.workload.repository.domain.Workload
import io.airbyte.workload.repository.domain.WorkloadLabel
import io.airbyte.workload.repository.domain.WorkloadStatus
import io.airbyte.workload.services.ConflictException
import io.airbyte.workload.services.NotFoundException
import io.airbyte.workload.services.WorkloadService
import io.micronaut.context.annotation.Property
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.nio.file.Path
import java.time.OffsetDateTime
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

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
  val mutexKey: String?,
)

@Singleton
class CommandService(
  private val actorRepository: ActorRepository,
  private val catalogService: CatalogService,
  private val commandsRepository: CommandsRepository,
  private val jobInputService: JobInputService,
  private val logClientManager: LogClientManager,
  private val organizationService: OrganizationService,
  private val workloadService: WorkloadService,
  private val workloadQueueService: WorkloadQueueService,
  private val workloadOutputReader: WorkloadOutputDocStoreReader,
  private val workloadIdGenerator: WorkloadIdGenerator,
  private val workspaceService: WorkspaceService,
  @Named("workspaceRoot") private val workspaceRoot: Path,
  @Property(name = "airbyte.worker.discover.auto-refresh-window") discoverAutoRefreshWindowMinutes: Int,
) {
  private val discoverAutoRefreshWindow: Duration =
    if (discoverAutoRefreshWindowMinutes > 0) discoverAutoRefreshWindowMinutes.minutes else Duration.INFINITE

  /** Create a Check command for an actorDefinitionId and a configuration
   *
   * returns true if a command has been created, false if it already existed.
   */
  fun createCheckCommand(
    commandId: String,
    actorDefinitionId: UUID,
    workspaceId: UUID,
    configuration: JsonNode,
    workloadPriority: WorkloadPriority,
    signalInput: String?,
    commandInput: JsonNode,
  ): Boolean {
    if (commandsRepository.existsById(commandId)) {
      return false
    }

    val checkInput = jobInputService.getCheckInput(actorDefinitionId, workspaceId, configuration)
    // Adding the priority to the launcherConfig because it impacts node-pool selection.
    checkInput.launcherConfig.priority = workloadPriority

    val workloadPayload =
      createCheckCreateWorkloadRequest(
        actorId = null,
        actorDefinitionId = actorDefinitionId,
        workspaceId = workspaceId,
        checkInput = checkInput,
        workloadPriority = workloadPriority,
        signalInput = signalInput,
      )
    createCommand(
      commandId = commandId,
      commandType = CommandType.CHECK.name,
      commandInput = commandInput,
      workspaceId = workspaceId,
      workloadPayload = workloadPayload,
    )
    return true
  }

  /** Create a Check command for an actorId
   *
   * returns true if a command has been created, false if it already existed.
   */
  fun createCheckCommand(
    commandId: String,
    actorId: UUID,
    jobId: String?,
    attemptNumber: Long?,
    workloadPriority: WorkloadPriority,
    signalInput: String?,
    commandInput: JsonNode,
  ): Boolean {
    if (commandsRepository.existsById(commandId)) {
      return false
    }

    val checkInput = jobInputService.getCheckInput(actorId, jobId, attemptNumber)
    // Adding the priority to the launcherConfig because it impacts node-pool selection.
    checkInput.launcherConfig.priority = workloadPriority

    val actor = actorRepository.findByActorId(actorId) ?: throw NotFoundException("Unable to find actorId $actorId")
    val workspaceId = actor.workspaceId
    val workloadPayload =
      createCheckCreateWorkloadRequest(
        actorId = actorId,
        actorDefinitionId = actor.actorDefinitionId,
        workspaceId = workspaceId,
        checkInput = checkInput,
        workloadPriority = workloadPriority,
        signalInput = signalInput,
      )
    createCommand(
      commandId = commandId,
      commandType = CommandType.CHECK.name,
      commandInput = commandInput,
      workspaceId = workspaceId,
      workloadPayload = workloadPayload,
    )
    return true
  }

  private fun createCheckCreateWorkloadRequest(
    actorId: UUID?,
    actorDefinitionId: UUID,
    workspaceId: UUID,
    checkInput: CheckConnectionInput,
    workloadPriority: WorkloadPriority,
    signalInput: String?,
  ): WorkloadCreatePayload {
    val jobId = checkInput.jobRunConfig.jobId
    val attemptNumber = checkInput.jobRunConfig.attemptId // because this is the only place where it's attemptId

    val workloadId =
      workloadIdGenerator.generateCheckWorkloadId(
        actorId = actorId,
        actorDefinitionId = actorDefinitionId,
        jobId = jobId,
        attemptNumber = attemptNumber,
      )
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
      mutexKey = null,
    )
  }

  fun createDiscoverCommand(
    commandId: String,
    actorId: UUID,
    jobId: String?,
    attemptNumber: Long?,
    workloadPriority: WorkloadPriority,
    signalInput: String?,
    commandInput: JsonNode,
  ): Boolean {
    if (commandsRepository.existsById(commandId)) {
      return false
    }

    val actualJobId = jobId ?: UUID.randomUUID().toString()
    val actualAttemptNumber = attemptNumber ?: 0L
    val actor = actorRepository.findByActorId(actorId) ?: throw NotFoundException("Unable to find actorId $actorId")
    val workspaceId = actor.workspaceId
    val discoverInput = jobInputService.getDiscoverInput(actorId, actualJobId, actualAttemptNumber)
    // Adding the priority to the launcherConfig because it impacts node-pool selection.
    discoverInput.integrationLauncherConfig.priority = workloadPriority

    val workloadPayload =
      createDiscoverWorkloadRequest(
        actorId = actorId,
        workspaceId = workspaceId,
        discoverInput = discoverInput,
        workloadPriority = workloadPriority,
        signalInput = signalInput,
      )
    createCommand(
      commandId = commandId,
      commandType = CommandType.DISCOVER.name,
      commandInput = commandInput,
      workspaceId = workspaceId,
      workloadPayload = workloadPayload,
    )
    return true
  }

  private fun createDiscoverWorkloadRequest(
    actorId: UUID,
    workspaceId: UUID,
    discoverInput: DiscoverCommandInput.DiscoverCatalogInput,
    workloadPriority: WorkloadPriority,
    signalInput: String?,
  ): WorkloadCreatePayload {
    val jobId = discoverInput.jobRunConfig.jobId
    val attemptNumber = discoverInput.jobRunConfig.attemptId // because this is the only place where it's attemptId

    val workloadId =
      workloadIdGenerator.generateDiscoverWorkloadId(
        actorId = actorId,
        jobId = jobId,
        attemptNumber = attemptNumber,
        isManual = discoverInput.discoverCatalogInput.manual,
        discoverAutoRefreshWindow = discoverAutoRefreshWindow,
      )
    val labels =
      listOfNotNull(
        WorkloadLabel(key = WorkloadLabels.JOB_LABEL_KEY, value = jobId),
        WorkloadLabel(key = WorkloadLabels.ATTEMPT_LABEL_KEY, value = attemptNumber.toString()),
        WorkloadLabel(key = WorkloadLabels.WORKSPACE_LABEL_KEY, value = workspaceId.toString()),
        WorkloadLabel(key = WorkloadLabels.ACTOR_TYPE, value = ActorType.SOURCE.toString()),
      )

    val dataplaneGroupId = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false).dataplaneGroupId

    return WorkloadCreatePayload(
      workloadId = workloadId,
      labels = labels,
      workloadInput =
        Jsons.serialize(
          // TODO This is because the current object we serialize is in a dependency that we do not want to pull in the server
          // TODO This should be changed once we decommissioned the old flow from the worker
          object {
            val jobRunConfig = discoverInput.jobRunConfig
            val launcherConfig = discoverInput.integrationLauncherConfig
            val discoverCatalogInput = discoverInput.discoverCatalogInput
          },
        ),
      logPath = logClientManager.fullLogPath(TemporalUtils.getJobRoot(workspaceRoot, jobId, attemptNumber)),
      type = WorkloadType.DISCOVER,
      priority = workloadPriority,
      dataplaneGroupId = dataplaneGroupId,
      signalInput = signalInput,
      mutexKey = null,
    )
  }

  fun createReplicateCommand(
    commandId: String,
    connectionId: UUID,
    jobId: String,
    attemptNumber: Long,
    appliedCatalogDiff: CatalogDiff?,
    signalInput: String?,
    commandInput: JsonNode,
  ): Boolean {
    if (commandsRepository.existsById(commandId)) {
      return false
    }

    val replicationInput =
      jobInputService.getReplicationInput(
        connectionId = connectionId,
        appliedCatalogDiff = appliedCatalogDiff,
        jobId = jobId.toLong(),
        attemptNumber = attemptNumber,
        signalInput = signalInput,
      )
    val workspaceId = replicationInput.connectionContext?.workspaceId ?: throw IllegalStateException("workspaceId is missing")
    val workloadPayload =
      createReplicateWorkloadRequest(
        connectionId = connectionId,
        workspaceId = workspaceId,
        replicationInput = replicationInput,
        signalInput = signalInput,
      )
    createCommand(
      commandId = commandId,
      commandType = CommandType.REPLICATE.name,
      commandInput = commandInput,
      workspaceId = workspaceId,
      workloadPayload = workloadPayload,
    )
    return true
  }

  private fun createReplicateWorkloadRequest(
    connectionId: UUID,
    workspaceId: UUID,
    replicationInput: ReplicationActivityInput,
    signalInput: String?,
  ): WorkloadCreatePayload {
    val jobId = replicationInput.jobRunConfig?.jobId?.toLong() ?: throw IllegalStateException("jobId is missing")
    // because this is the only place where it's attemptId
    val attemptNumber = replicationInput.jobRunConfig?.attemptId?.toLong() ?: throw IllegalStateException("attemptNumber is missing")

    val workloadId =
      workloadIdGenerator.generateReplicateWorkloadId(
        connectionId = connectionId,
        jobId = jobId,
        attemptNumber = attemptNumber,
      )
    val labels =
      listOfNotNull(
        WorkloadLabel(key = WorkloadLabels.CONNECTION_ID_LABEL_KEY, value = connectionId.toString()),
        WorkloadLabel(key = WorkloadLabels.JOB_LABEL_KEY, value = jobId.toString()),
        WorkloadLabel(key = WorkloadLabels.ATTEMPT_LABEL_KEY, value = attemptNumber.toString()),
        WorkloadLabel(key = WorkloadLabels.WORKSPACE_LABEL_KEY, value = workspaceId.toString()),
        WorkloadLabel(key = WorkloadLabels.ACTOR_TYPE, value = ActorType.SOURCE.toString()),
        WorkloadLabel(key = WorkloadLabels.WORKER_POD_LABEL_KEY, value = WorkloadLabels.WORKER_POD_LABEL_VALUE),
      )

    val dataplaneGroupId = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false).dataplaneGroupId

    return WorkloadCreatePayload(
      workloadId = workloadId,
      labels = labels,
      workloadInput = Jsons.serialize(replicationInput),
      logPath = logClientManager.fullLogPath(TemporalUtils.getJobRoot(workspaceRoot, jobId.toString(), attemptNumber)),
      type = WorkloadType.SYNC,
      priority = WorkloadPriority.DEFAULT,
      dataplaneGroupId = dataplaneGroupId,
      signalInput = signalInput,
      mutexKey = connectionId.toString(),
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
    val workloadAutoId = UUID.randomUUID()

    // TODO the workloadService.createWorkload and Command.save should be in a transaction
    val createdWorkload =
      try {
        workloadService.createWorkload(
          workloadId = workloadPayload.workloadId,
          labels = workloadPayload.labels,
          input = workloadPayload.workloadInput,
          workspaceId = workspaceId,
          organizationId = organizationId,
          logPath = workloadPayload.logPath,
          mutexKey = workloadPayload.mutexKey,
          type = workloadPayload.type,
          autoId = workloadAutoId,
          deadline = null,
          signalInput = workloadPayload.signalInput,
          dataplaneGroup = workloadPayload.dataplaneGroupId.toString(),
          priority = workloadPayload.priority,
        )
        true
      } catch (e: ConflictException) {
        log.info { "The workload ${workloadPayload.workloadId} already exists for command $commandId, continuing" }
        false
      }

    // because workloadId is a foreign key
    commandsRepository.save(command)

    // TODO this should only run if both saves above are successful
    if (createdWorkload) {
      workloadQueueService.create(
        workloadId = workloadPayload.workloadId,
        workloadInput = workloadPayload.workloadInput,
        labels = workloadPayload.labels.associate { it.key to it.value },
        logPath = workloadPayload.logPath,
        mutexKey = workloadPayload.mutexKey,
        workloadType = workloadPayload.type,
        autoId = workloadAutoId,
        priority = workloadPayload.priority,
        dataplaneGroup = workloadPayload.dataplaneGroupId.toString(),
      )
    }
  }

  fun cancel(commandId: String) {
    commandsRepository.findById(commandId).ifPresent { command ->
      workloadService.cancelWorkload(command.workloadId, "api", "cancelled from the api")
    }
  }

  data class CommandModel(
    val id: String,
    val workloadId: String,
    val commandType: String,
    val commandInput: JsonNode,
    val workspaceId: UUID,
    val organizationId: UUID,
    val createdAt: OffsetDateTime,
    val updatedAt: OffsetDateTime,
  )

  fun get(commandId: String): CommandModel? =
    commandsRepository
      .findById(commandId)
      .map { command ->
        CommandModel(
          id = command.id,
          workloadId = command.workloadId,
          commandType = command.commandType,
          commandInput = command.commandInput,
          workspaceId = command.workspaceId,
          organizationId = command.organizationId,
          createdAt = command.createdAt ?: OffsetDateTime.now(),
          updatedAt = command.updatedAt ?: OffsetDateTime.now(),
        )
      }.orElse(null)

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

  fun getCheckJobOutput(commandId: String): ConnectorJobOutput? =
    getConnectorJobOutput(commandId) { failureReason ->
      ConnectorJobOutput()
        .withOutputType(ConnectorJobOutput.OutputType.CHECK_CONNECTION)
        .withCheckConnection(
          StandardCheckConnectionOutput()
            .withStatus(StandardCheckConnectionOutput.Status.FAILED)
            .withMessage(failureReason.externalMessage),
        ).withFailureReason(failureReason)
    }

  data class DiscoverJobOutput(
    val catalogId: UUID?,
    val catalog: ActorCatalog?,
    val failureReason: FailureReason?,
  )

  fun getDiscoverJobOutput(commandId: String): DiscoverJobOutput? =
    getConnectorJobOutput(commandId) { failureReason ->
      ConnectorJobOutput()
        .withOutputType(ConnectorJobOutput.OutputType.DISCOVER_CATALOG_ID)
        .withDiscoverCatalogId(null)
        .withFailureReason(failureReason)
    }?.let { jobOutput ->
      val catalog = jobOutput.discoverCatalogId?.let { catalogService.getActorCatalogById(it) }
      return DiscoverJobOutput(
        catalogId = jobOutput.discoverCatalogId,
        catalog = catalog,
        failureReason = jobOutput.failureReason,
      )
    }

  fun getReplicationOutput(commandId: String): ReplicationOutput? =
    getReplicationOutput(commandId) { failureReason ->
      ReplicationOutput().withFailures(listOf(failureReason))
    }

  private fun getConnectorJobOutput(
    commandId: String,
    onFailure: (FailureReason) -> ConnectorJobOutput,
  ): ConnectorJobOutput? =
    commandsRepository
      .findById(commandId)
      .map { command ->
        try {
          workloadOutputReader.readConnectorOutput(command.workloadId) ?: throw NotFoundException("no output found for $commandId")
        } catch (e: Exception) {
          val workload = workloadService.getWorkload(command.workloadId)
          val failureReason = getFailureReasonForMissingConnectorJobOutput(commandId, workload, e)
          onFailure(failureReason)
        }
      }.orElse(null)

  private fun getReplicationOutput(
    commandId: String,
    onFailure: (FailureReason) -> ReplicationOutput,
  ): ReplicationOutput? =
    commandsRepository
      .findById(commandId)
      .map { command ->
        try {
          workloadOutputReader.readSyncOutput(command.workloadId) ?: throw NotFoundException("no output found for $commandId")
        } catch (e: Exception) {
          val workload = workloadService.getWorkload(command.workloadId)
          val failureReason = getFailureReasonForMissingConnectorJobOutput(commandId, workload, e)
          onFailure(failureReason)
        }
      }.orElse(null)

  private fun getFailureReasonForMissingConnectorJobOutput(
    commandId: String,
    workload: Workload,
    e: Exception?,
  ): FailureReason =
    when (workload.status) {
      // This is pretty bad, the workload succeeded, but we failed to read the output
      WorkloadStatus.SUCCESS ->
        FailureReason()
          .withFailureOrigin(FailureReason.FailureOrigin.AIRBYTE_PLATFORM)
          .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)
          .withExternalMessage("Failed to read the output")
          .withInternalMessage("Failed to read the output of a successful workload $commandId")
          .withStacktrace(e?.stackTraceToString())

      // do some classification from workload.terminationSource
      WorkloadStatus.CANCELLED, WorkloadStatus.FAILURE ->
        FailureReason()
          .withFailureOrigin(
            when (workload.terminationSource) {
              "source" -> FailureReason.FailureOrigin.SOURCE
              "destination" -> FailureReason.FailureOrigin.DESTINATION
              else -> FailureReason.FailureOrigin.AIRBYTE_PLATFORM
            },
          ).withExternalMessage(
            "Workload ${if (workload.status == WorkloadStatus.CANCELLED) "cancelled by" else "failed, source:"} ${workload.terminationSource}",
          ).withInternalMessage(workload.terminationReason)

      // We should never be in this situation, workload is still running not having an output is expected,
      // we should not be trying to read the output of a non-terminal workload.
      else ->
        FailureReason()
          .withFailureOrigin(FailureReason.FailureOrigin.AIRBYTE_PLATFORM)
          .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)
          .withExternalMessage("$commandId is still running, try again later.")
          .withInternalMessage("$commandId isn't in a terminal state, no output available")
    }
}
