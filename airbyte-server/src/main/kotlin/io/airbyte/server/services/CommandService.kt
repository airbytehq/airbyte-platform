/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.logging.LogClientManager
import io.airbyte.commons.logging.LogEvents
import io.airbyte.commons.server.converters.ConfigurationUpdate
import io.airbyte.commons.server.helpers.SecretSanitizer
import io.airbyte.commons.temporal.TemporalUtils
import io.airbyte.commons.temporal.scheduling.DiscoverCommandInput
import io.airbyte.config.ActorCatalog
import io.airbyte.config.ActorType
import io.airbyte.config.CatalogDiff
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.ReplicationAttemptSummary
import io.airbyte.config.ReplicationOutput
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.WorkloadPriority
import io.airbyte.config.WorkloadType
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.repositories.ActorRepository
import io.airbyte.data.services.CatalogService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.featureflag.Empty
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.ReplicationCommandFallsBackToWorkloadStatus
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.micronaut.runtime.AirbyteWorkerConfig
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.server.helpers.WorkloadIdGenerator
import io.airbyte.server.repositories.CommandsRepository
import io.airbyte.server.repositories.domain.Command
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workers.models.SpecInput
import io.airbyte.workload.common.WorkloadLabels
import io.airbyte.workload.common.WorkloadQueueService
import io.airbyte.workload.output.WorkloadOutputDocStoreReader
import io.airbyte.workload.repository.domain.Workload
import io.airbyte.workload.repository.domain.WorkloadLabel
import io.airbyte.workload.repository.domain.WorkloadStatus
import io.airbyte.workload.services.ConflictException
import io.airbyte.workload.services.InvalidStatusTransitionException
import io.airbyte.workload.services.NotFoundException
import io.airbyte.workload.services.WorkloadService
import jakarta.inject.Singleton
import java.nio.file.Path
import java.time.Clock
import java.time.OffsetDateTime
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType as JooqActorType

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
  SPEC,
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
  private val secretSanitizer: SecretSanitizer,
  private val configurationUpdate: ConfigurationUpdate,
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
  private val actorDefinitionVersionHelper: ActorDefinitionVersionHelper,
  private val airbyteConfig: AirbyteConfig,
  airbyteWorkerConfig: AirbyteWorkerConfig,
  private val featureFlagClient: FeatureFlagClient,
  clock: Clock?,
) {
  private val clock: Clock = clock ?: Clock.systemUTC()

  private val discoverAutoRefreshWindow: Duration =
    if (airbyteWorkerConfig.discover.autoRefreshWindow >
      0
    ) {
      airbyteWorkerConfig.discover.autoRefreshWindow.minutes
    } else {
      Duration.INFINITE
    }

  fun createSpecCommand(
    commandId: String,
    dockerImage: String,
    dockerImageTag: String,
    workspaceId: UUID,
    signalInput: String?,
    commandInput: JsonNode,
  ): Boolean {
    if (commandsRepository.existsById(commandId)) {
      return false
    }

    val specInput =
      jobInputService.getSpecInput(
        dockerImage = dockerImage,
        dockerImageTag = dockerImageTag,
        workspaceId = workspaceId,
        jobId = null,
        attemptId = null,
        isCustomConnector = true,
      )
    // Spec commands always use HIGH priority
    val priority = WorkloadPriority.HIGH
    specInput.launcherConfig.priority = priority

    val workloadPayload =
      createSpecWorkloadRequest(
        workspaceId = workspaceId,
        specInput = specInput,
        workloadPriority = priority,
        signalInput = signalInput,
      )

    createCommand(
      commandId = commandId,
      commandType = CommandType.SPEC.name,
      commandInput = commandInput,
      workspaceId = workspaceId,
      workloadPayload = workloadPayload,
    )
    return true
  }

  fun createSpecCommand(
    commandId: String,
    actorDefinitionId: UUID,
    dockerImageTag: String,
    workspaceId: UUID,
    signalInput: String?,
    commandInput: JsonNode,
  ): Boolean {
    if (commandsRepository.existsById(commandId)) {
      return false
    }

    val specInput =
      jobInputService.getSpecInput(
        actorDefinitionId = actorDefinitionId,
        dockerImageTag = dockerImageTag,
        workspaceId = workspaceId,
        jobId = null,
        attemptId = null,
      )
    // Spec commands always use HIGH priority
    val priority = WorkloadPriority.HIGH
    specInput.launcherConfig.priority = priority

    val workloadPayload =
      createSpecWorkloadRequest(
        workspaceId = workspaceId,
        specInput = specInput,
        workloadPriority = priority,
        signalInput = signalInput,
      )

    createCommand(
      commandId = commandId,
      commandType = CommandType.SPEC.name,
      commandInput = commandInput,
      workspaceId = workspaceId,
      workloadPayload = workloadPayload,
    )
    return true
  }

  private fun createSpecWorkloadRequest(
    workspaceId: UUID,
    specInput: SpecInput,
    workloadPriority: WorkloadPriority,
    signalInput: String?,
  ): WorkloadCreatePayload {
    val jobId = specInput.jobRunConfig.jobId
    val attemptNumber = specInput.jobRunConfig.attemptId

    val workloadId = workloadIdGenerator.generateSpecWorkloadId(jobId = jobId)

    val labels =
      listOf(
        WorkloadLabel(key = WorkloadLabels.JOB_LABEL_KEY, value = jobId),
        WorkloadLabel(key = WorkloadLabels.ATTEMPT_LABEL_KEY, value = attemptNumber.toString()),
        WorkloadLabel(key = WorkloadLabels.WORKSPACE_LABEL_KEY, value = workspaceId.toString()),
      )

    val dataplaneGroupId = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false).dataplaneGroupId

    return WorkloadCreatePayload(
      workloadId = workloadId,
      labels = labels,
      workloadInput = Jsons.serialize(specInput),
      logPath = logClientManager.fullLogPath(Path.of(workloadId)),
      type = WorkloadType.SPEC,
      priority = workloadPriority,
      dataplaneGroupId = dataplaneGroupId,
      signalInput = signalInput,
      mutexKey = null,
    )
  }

  /**
   * Helper data class to hold actor context information for check commands.
   */
  data class CheckCommandContext(
    val actorDefinitionId: UUID,
    val workspaceId: UUID,
  )

  /**
   * Gets the actor definition ID and workspace ID for a check command request.
   * This is used by the controller layer for config sanitization before calling createCheckCommand.
   *
   * @param actorId Optional actor ID
   * @param actorDefinitionId Optional actor definition ID (required if actorId is null)
   * @param workspaceId Optional workspace ID (required if actorId is null)
   * @return CheckCommandContext containing the definitionId and workspaceId
   */
  fun getCheckCommandContext(
    actorId: UUID? = null,
    actorDefinitionId: UUID? = null,
    workspaceId: UUID? = null,
  ): CheckCommandContext {
    // If actorId is provided, fetch both values in a single query
    if (actorId != null) {
      val actor =
        actorRepository.findByActorId(actorId)
          ?: throw NotFoundException("Unable to find actorId $actorId")
      return CheckCommandContext(
        actorDefinitionId = actor.actorDefinitionId,
        workspaceId = actor.workspaceId,
      )
    }

    // Otherwise, use provided values
    if (actorDefinitionId != null && workspaceId != null) {
      return CheckCommandContext(
        actorDefinitionId = actorDefinitionId,
        workspaceId = workspaceId,
      )
    }

    throw IllegalArgumentException("Must provide either actorId OR (actorDefinitionId + workspaceId)")
  }

  /**
   * Processes and sanitizes configuration for check commands.
   *
   * In hybrid mode (actorId + config provided):
   * 1. Fetches stored config
   * 2. Copies secrets from stored config to handle masked values like "**********"
   * 3. Sanitizes the merged config
   *
   * In non-hybrid mode (just config):
   * 1. Sanitizes the provided config directly
   *
   * @param actorId Optional actor ID (for hybrid mode)
   * @param actorDefinitionId Actor definition ID
   * @param workspaceId Workspace ID
   * @param providedConfig The configuration from the request
   * @return Sanitized configuration with secret references
   */
  fun processCheckConfig(
    actorId: UUID?,
    actorDefinitionId: UUID,
    workspaceId: UUID,
    providedConfig: JsonNode,
  ): JsonNode {
    // In hybrid mode, copy secrets from stored config first
    // This handles masked values like "**********" properly
    val configWithCopiedSecrets =
      if (actorId != null) {
        val actor =
          actorRepository.findByActorId(actorId)
            ?: throw NotFoundException("Unable to find actorId $actorId")

        // Get stored config based on actor type
        val storedConfig =
          when (actor.actorType) {
            JooqActorType.source ->
              sourceService.getSourceConnection(actorId).configuration
            JooqActorType.destination ->
              destinationService.getDestinationConnection(actorId).configuration
          }

        // Get connector spec for secret identification
        val spec = getConnectionSpec(actorDefinitionId, workspaceId)

        // Copy secrets from stored config (handles "**********" mask)
        configurationUpdate.secretsProcessor.copySecrets(
          storedConfig,
          providedConfig,
          spec.connectionSpecification,
        )
      } else {
        providedConfig
      }

    // Sanitize config (with copied secrets if hybrid mode)
    return secretSanitizer.sanitizePartialConfig(
      actorDefinitionId = actorDefinitionId,
      workspaceId = workspaceId,
      connectionConfiguration = configWithCopiedSecrets,
    )
  }

  /**
   * Helper to get connector specification for an actor definition.
   * Tries source first, then destination.
   */
  @InternalForTesting
  internal fun getConnectionSpec(
    actorDefinitionId: UUID,
    workspaceId: UUID,
  ): ConnectorSpecification {
    try {
      return getSourceConnectionSpec(actorDefinitionId, workspaceId)
    } catch (e: ConfigNotFoundException) {
      return getDestinationConnectionSpec(actorDefinitionId, workspaceId)
    }
  }

  private fun getSourceConnectionSpec(
    actorDefinitionId: UUID,
    workspaceId: UUID,
  ): ConnectorSpecification {
    val sourceDef = sourceService.getStandardSourceDefinition(actorDefinitionId)
    val sourceVersion = actorDefinitionVersionHelper.getSourceVersion(sourceDef, workspaceId)
    return sourceVersion.spec
  }

  private fun getDestinationConnectionSpec(
    actorDefinitionId: UUID,
    workspaceId: UUID,
  ): ConnectorSpecification {
    val destDef = destinationService.getStandardDestinationDefinition(actorDefinitionId)
    val destVersion = actorDefinitionVersionHelper.getDestinationVersion(destDef, workspaceId)
    return destVersion.spec
  }

  /**
   * Unified method to create a Check command supporting all modes:
   * 1. Actor ID only (uses stored config)
   * 2. Definition ID + config (no stored actor)
   * 3. Actor ID + config override (hybrid: stored credentials + provided config)
   *
   * returns true if a command has been created, false if it already existed.
   */
  fun createCheckCommand(
    commandId: String,
    actorId: UUID? = null,
    actorDefinitionId: UUID? = null,
    workspaceId: UUID? = null,
    configuration: JsonNode? = null,
    jobId: String? = null,
    attemptNumber: Long? = null,
    workloadPriority: WorkloadPriority,
    signalInput: String?,
    commandInput: JsonNode,
  ): Boolean {
    if (commandsRepository.existsById(commandId)) {
      return false
    }

    val checkInput =
      jobInputService.getCheckInput(
        actorId = actorId,
        actorDefinitionId = actorDefinitionId,
        workspaceId = workspaceId,
        configuration = configuration,
        jobId = jobId,
        attemptId = attemptNumber,
      )

    // Adding the priority to the launcherConfig because it impacts node-pool selection
    checkInput.launcherConfig.priority = workloadPriority

    // Determine workspace and actor details
    val effectiveActorId = actorId
    val effectiveWorkspaceId =
      if (workspaceId != null) {
        workspaceId
      } else if (actorId != null) {
        actorRepository.findByActorId(actorId)?.workspaceId
          ?: throw NotFoundException("Unable to find actorId $actorId")
      } else {
        throw IllegalArgumentException("Must provide either actorId or workspaceId")
      }

    val effectiveActorDefinitionId =
      if (actorDefinitionId != null) {
        actorDefinitionId
      } else if (actorId != null) {
        actorRepository.findByActorId(actorId)?.actorDefinitionId
          ?: throw NotFoundException("Unable to find actorId $actorId")
      } else {
        throw IllegalArgumentException("Must provide either actorId or actorDefinitionId")
      }

    val workloadPayload =
      createCheckCreateWorkloadRequest(
        actorId = effectiveActorId,
        actorDefinitionId = effectiveActorDefinitionId,
        workspaceId = effectiveWorkspaceId,
        checkInput = checkInput,
        workloadPriority = workloadPriority,
        signalInput = signalInput,
      )

    createCommand(
      commandId = commandId,
      commandType = CommandType.CHECK.name,
      commandInput = commandInput,
      workspaceId = effectiveWorkspaceId,
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
      logPath = logClientManager.fullLogPath(TemporalUtils.getJobRoot(Path.of(airbyteConfig.workspaceRoot), jobId, attemptNumber)),
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

    val actor = actorRepository.findByActorId(actorId) ?: throw NotFoundException("Unable to find actorId $actorId")
    val workspaceId = actor.workspaceId
    val discoverInput = jobInputService.getDiscoverInput(actorId, jobId, attemptNumber)
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
      logPath = logClientManager.fullLogPath(TemporalUtils.getJobRoot(Path.of(airbyteConfig.workspaceRoot), jobId, attemptNumber)),
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
      logPath = logClientManager.fullLogPath(TemporalUtils.getJobRoot(Path.of(airbyteConfig.workspaceRoot), jobId.toString(), attemptNumber)),
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
      try {
        workloadService.cancelWorkload(command.workloadId, "api", "cancelled from the api")
      } catch (e: InvalidStatusTransitionException) {
        log.info(e) { "Trying to cancel $commandId that has an already terminated workload ${command.workloadId}." }
      }
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

  fun getJobLogs(commandId: String): JobLogs {
    val command = commandsRepository.findById(commandId).orElse(null) ?: return JobLogs()
    val workload =
      try {
        workloadService.getWorkload(command.workloadId)
      } catch (e: Exception) {
        log.warn(e) { "Unable to find workload:${command.workloadId} for command:$commandId" }
        return JobLogs.empty()
      }

    val logPath = Path.of(workload.logPath)
    try {
      val logEvents = logClientManager.getLogs(logPath)
      if (logEvents.events.isNotEmpty()) {
        return JobLogs.createStructuredLogs(logEvents = logEvents)
      } else {
        val logLines = logClientManager.getJobLogFile(logPath)
        return JobLogs.createFormattedLogs(rawLogs = logLines)
      }
    } catch (e: Exception) {
      log.warn(e) { "Unable to find logs for command:$commandId workloadId:${command.workloadId}" }
      return JobLogs.empty()
    }
  }

  fun getCheckJobOutput(
    commandId: String,
    withLogs: Boolean,
  ): CheckJobOutput? =
    getConnectorJobOutput(commandId) { failureReason ->
      ConnectorJobOutput()
        .withOutputType(ConnectorJobOutput.OutputType.CHECK_CONNECTION)
        .withCheckConnection(
          StandardCheckConnectionOutput()
            .withStatus(StandardCheckConnectionOutput.Status.FAILED)
            .withMessage(failureReason.externalMessage),
        ).withFailureReason(failureReason)
    }?.let { jobOutput ->
      return CheckJobOutput(
        status = jobOutput.checkConnection.status,
        connectorConfigUpdated = jobOutput.connectorConfigurationUpdated ?: false,
        message = jobOutput.checkConnection?.message,
        failureReason = jobOutput.failureReason,
        logs = if (withLogs) getJobLogs(commandId) else null,
      )
    }

  data class JobLogs(
    val logEvents: LogEvents? = null,
    val logLines: List<String>? = null,
  ) {
    fun isStructured(): Boolean = logEvents != null || logLines == null

    companion object {
      fun createStructuredLogs(logEvents: LogEvents): JobLogs = JobLogs(logEvents = logEvents)

      fun createFormattedLogs(rawLogs: List<String>): JobLogs = JobLogs(logLines = rawLogs)

      fun empty(): JobLogs = JobLogs(logEvents = null)
    }
  }

  data class CheckJobOutput(
    val status: StandardCheckConnectionOutput.Status,
    val connectorConfigUpdated: Boolean,
    val message: String?,
    val failureReason: FailureReason?,
    val logs: JobLogs?,
  )

  data class SpecJobOutput(
    val spec: ConnectorSpecification?,
    val failureReason: FailureReason?,
    val logs: JobLogs?,
  )

  fun getSpecJobOutput(
    commandId: String,
    withLogs: Boolean,
  ): SpecJobOutput? =
    getConnectorJobOutput(commandId) { failureReason ->
      ConnectorJobOutput()
        .withOutputType(ConnectorJobOutput.OutputType.SPEC)
        .withSpec(null)
        .withFailureReason(failureReason)
    }?.let { jobOutput ->
      return SpecJobOutput(
        spec = jobOutput.spec,
        failureReason = jobOutput.failureReason,
        logs = if (withLogs) getJobLogs(commandId) else null,
      )
    }

  data class DiscoverJobOutput(
    val catalogId: UUID?,
    val catalog: ActorCatalog?,
    val destinationCatalog: ActorCatalog?,
    val failureReason: FailureReason?,
    val logs: JobLogs?,
  )

  fun getDiscoverJobOutput(
    commandId: String,
    withLogs: Boolean,
  ): DiscoverJobOutput? =
    getConnectorJobOutput(commandId) { failureReason ->
      ConnectorJobOutput()
        .withOutputType(ConnectorJobOutput.OutputType.DISCOVER_CATALOG_ID)
        .withDiscoverCatalogId(null)
        .withFailureReason(failureReason)
    }?.let { jobOutput ->
      val actorCatalog = jobOutput.discoverCatalogId?.let { catalogService.getActorCatalogById(it) }
      val isDestinationCatalog = actorCatalog?.catalogType == ActorCatalog.CatalogType.DESTINATION_CATALOG

      return DiscoverJobOutput(
        catalogId = jobOutput.discoverCatalogId,
        catalog = if (isDestinationCatalog) null else actorCatalog,
        destinationCatalog = if (isDestinationCatalog) actorCatalog else null,
        failureReason = jobOutput.failureReason,
        logs = if (withLogs) getJobLogs(commandId) else null,
      )
    }

  fun getReplicationOutput(commandId: String): ReplicationOutput? =
    commandsRepository
      .findById(commandId)
      .map { command ->
        try {
          workloadOutputReader.readSyncOutput(command.workloadId) ?: throw NotFoundException("no output found for $commandId")
        } catch (e: Exception) {
          val workload = workloadService.getWorkload(command.workloadId)

          val failureReason = getFailureReasonForMissingConnectorJobOutput(commandId, workload, e)

          val fallbackOutput = ReplicationOutput().withFailures(listOf(failureReason))

          if (featureFlagClient.boolVariation(ReplicationCommandFallsBackToWorkloadStatus, Empty)) {
            // Fallback to the workload status when we can't read the output
            if (workload.status == WorkloadStatus.FAILURE) {
              fallbackOutput.replicationAttemptSummary =
                ReplicationAttemptSummary()
                  .withStatus(StandardSyncSummary.ReplicationStatus.FAILED)
            }
            if (workload.status == WorkloadStatus.CANCELLED) {
              fallbackOutput.replicationAttemptSummary =
                ReplicationAttemptSummary()
                  .withStatus(StandardSyncSummary.ReplicationStatus.CANCELLED)
            }
            if (workload.status == WorkloadStatus.SUCCESS) {
              fallbackOutput.replicationAttemptSummary =
                ReplicationAttemptSummary()
                  .withStatus(StandardSyncSummary.ReplicationStatus.COMPLETED)
            }
          }

          fallbackOutput
        }
      }.orElse(null)

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
          .withTimestamp(clock.millis())

      // do some classification from workload.terminationSource
      WorkloadStatus.CANCELLED, WorkloadStatus.FAILURE -> {
        val isImagePullError = workload.terminationReason?.contains("Failed to pull container image", ignoreCase = true) ?: false

        FailureReason()
          .withFailureOrigin(
            when (workload.terminationSource) {
              "source" -> FailureReason.FailureOrigin.SOURCE
              "destination" -> FailureReason.FailureOrigin.DESTINATION
              else -> FailureReason.FailureOrigin.AIRBYTE_PLATFORM
            },
          ).withFailureType(
            if (isImagePullError) {
              FailureReason.FailureType.CONFIG_ERROR
            } else {
              FailureReason.FailureType.SYSTEM_ERROR
            },
          ).withExternalMessage(workload.terminationReason)
          .withInternalMessage(workload.terminationReason)
          .withTimestamp(clock.millis())
      }

      // We should never be in this situation, workload is still running not having an output is expected,
      // we should not be trying to read the output of a non-terminal workload.
      else ->
        FailureReason()
          .withFailureOrigin(FailureReason.FailureOrigin.AIRBYTE_PLATFORM)
          .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)
          .withExternalMessage("$commandId is still running, try again later.")
          .withInternalMessage("$commandId isn't in a terminal state, no output available")
          .withTimestamp(clock.millis())
    }
}
