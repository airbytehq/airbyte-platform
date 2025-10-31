/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.AttemptInfoReadLogs
import io.airbyte.api.model.generated.CheckConnectionRead
import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.ConnectionStream
import io.airbyte.api.model.generated.ConnectionStreamRequestBody
import io.airbyte.api.model.generated.DestinationCoreConfig
import io.airbyte.api.model.generated.DestinationIdRequestBody
import io.airbyte.api.model.generated.DestinationUpdate
import io.airbyte.api.model.generated.JobConfigType
import io.airbyte.api.model.generated.JobCreate
import io.airbyte.api.model.generated.JobIdRequestBody
import io.airbyte.api.model.generated.JobInfoRead
import io.airbyte.api.model.generated.LogFormatType
import io.airbyte.api.model.generated.SourceAutoPropagateChange
import io.airbyte.api.model.generated.SourceCoreConfig
import io.airbyte.api.model.generated.SourceDiscoverSchemaRead
import io.airbyte.api.model.generated.SourceDiscoverSchemaRequestBody
import io.airbyte.api.model.generated.SourceIdRequestBody
import io.airbyte.api.model.generated.SourceUpdate
import io.airbyte.api.model.generated.SynchronousJobRead
import io.airbyte.api.model.generated.WorkloadPriority
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.security.md5
import io.airbyte.commons.server.converters.ConfigurationUpdate
import io.airbyte.commons.server.converters.JobConverter
import io.airbyte.commons.server.errors.NotFoundException
import io.airbyte.commons.server.errors.ValueConflictKnownException
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.commons.server.handlers.helpers.ConnectionTimelineEventHelper
import io.airbyte.commons.server.handlers.helpers.JobCreationAndStatusUpdateHelper
import io.airbyte.commons.server.helpers.SecretSanitizer
import io.airbyte.commons.server.scheduler.EventRunner
import io.airbyte.commons.server.scheduler.SynchronousResponse
import io.airbyte.commons.server.scheduler.SynchronousSchedulerClient
import io.airbyte.commons.temporal.ErrorCode
import io.airbyte.commons.temporal.ManualOperationResult
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.DestinationConnection
import io.airbyte.config.JobTypeResourceLimit
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.WorkloadPriority.Companion.fromValue
import io.airbyte.config.helpers.ResourceRequirementsUtils.getResourceRequirementsForJobType
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.persistence.StreamResetPersistence
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.CatalogService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.persistence.job.JobCreator
import io.airbyte.persistence.job.JobNotifier
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.persistence.job.factory.OAuthConfigSupplier
import io.airbyte.persistence.job.factory.SyncJobFactory
import io.airbyte.persistence.job.tracker.JobTracker
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.validation.json.JsonSchemaValidator
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.nio.charset.StandardCharsets
import java.util.UUID

private val log = KotlinLogging.logger {}

/**
 * ScheduleHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
open class SchedulerHandler
  @InternalForTesting
  constructor(
    actorDefinitionService: ActorDefinitionService,
    private val catalogService: CatalogService,
    private val connectionService: ConnectionService,
    private val synchronousSchedulerClient: SynchronousSchedulerClient,
    private val configurationUpdate: ConfigurationUpdate,
    private val jsonSchemaValidator: JsonSchemaValidator,
    private val jobPersistence: JobPersistence,
    private val eventRunner: EventRunner,
    private val jobConverter: JobConverter,
    private val connectionsHandler: ConnectionsHandler,
    private val actorDefinitionVersionHelper: ActorDefinitionVersionHelper,
    private val featureFlagClient: FeatureFlagClient,
    private val streamResetPersistence: StreamResetPersistence,
    private val oAuthConfigSupplier: OAuthConfigSupplier,
    private val jobCreator: JobCreator,
    private val jobFactory: SyncJobFactory,
    jobNotifier: JobNotifier,
    jobTracker: JobTracker,
    private val workspaceService: WorkspaceService,
    private val streamRefreshesHandler: StreamRefreshesHandler,
    private val connectionTimelineEventHelper: ConnectionTimelineEventHelper,
    private val sourceService: SourceService,
    private val destinationService: DestinationService,
    private val catalogConverter: CatalogConverter,
    private val metricClient: MetricClient,
    private val secretSanitizer: SecretSanitizer,
  ) {
    private val jobCreationAndStatusUpdateHelper =
      JobCreationAndStatusUpdateHelper(
        jobPersistence,
        actorDefinitionService,
        connectionService,
        jobNotifier,
        jobTracker,
        connectionTimelineEventHelper,
        metricClient,
      )

    fun checkSourceConnectionFromSourceId(sourceIdRequestBody: SourceIdRequestBody): CheckConnectionRead {
      val sourceId = sourceIdRequestBody.sourceId
      val source = sourceService.getSourceConnection(sourceId)
      val sourceDef = sourceService.getStandardSourceDefinition(source.sourceDefinitionId)
      val sourceVersion = actorDefinitionVersionHelper.getSourceVersion(sourceDef, source.workspaceId, sourceId)
      val isCustomConnector = sourceDef.custom
      // ResourceRequirements are read from actor definition and can be null; but if it's not null it will
      // have higher priority and overwrite
      // the default settings in WorkerConfig.
      val resourceRequirements =
        getResourceRequirementsForJobType(sourceDef.resourceRequirements, JobTypeResourceLimit.JobType.CHECK_CONNECTION)

      return reportConnectionStatus(
        synchronousSchedulerClient.createSourceCheckConnectionJob(source, sourceVersion, isCustomConnector, resourceRequirements),
      )
    }

    fun checkSourceConnectionFromSourceCreate(sourceConfig: SourceCoreConfig): CheckConnectionRead {
      val sourceDef = sourceService.getStandardSourceDefinition(sourceConfig.sourceDefinitionId)
      val sourceVersion =
        actorDefinitionVersionHelper.getSourceVersion(sourceDef, sourceConfig.workspaceId, sourceConfig.sourceId)
      // split out secrets
      val partialConfig =
        secretSanitizer.sanitizePartialConfig(
          sourceConfig.workspaceId,
          sourceConfig.connectionConfiguration,
          sourceVersion.spec,
        )

      // todo (cgardens) - narrow the struct passed to the client. we are not setting fields that are
      // technically declared as required.
      val source =
        SourceConnection()
          .withSourceId(sourceConfig.sourceId)
          .withSourceDefinitionId(sourceConfig.sourceDefinitionId)
          .withConfiguration(partialConfig)
          .withWorkspaceId(sourceConfig.workspaceId)

      val isCustomConnector = sourceDef.custom
      // ResourceRequirements are read from actor definition and can be null; but if it's not null it will
      // have higher priority and overwrite
      // the default settings in WorkerConfig.
      val resourceRequirements =
        getResourceRequirementsForJobType(sourceDef.resourceRequirements, JobTypeResourceLimit.JobType.CHECK_CONNECTION)

      return reportConnectionStatus(
        synchronousSchedulerClient.createSourceCheckConnectionJob(source, sourceVersion, isCustomConnector, resourceRequirements),
      )
    }

    fun checkSourceConnectionFromSourceIdForUpdate(sourceUpdate: SourceUpdate): CheckConnectionRead {
      val updatedSource =
        configurationUpdate.source(sourceUpdate.sourceId, sourceUpdate.name, sourceUpdate.connectionConfiguration)

      val sourceDef = sourceService.getStandardSourceDefinition(updatedSource.sourceDefinitionId)
      val sourceVersion =
        actorDefinitionVersionHelper.getSourceVersion(sourceDef, updatedSource.workspaceId, updatedSource.sourceId)

      // validate the provided updated config
      jsonSchemaValidator.ensure(
        sourceVersion.spec.connectionSpecification,
        sourceUpdate.connectionConfiguration,
      )

      val sourceCoreConfig =
        SourceCoreConfig()
          .sourceId(updatedSource.sourceId)
          .connectionConfiguration(updatedSource.configuration)
          .sourceDefinitionId(updatedSource.sourceDefinitionId)
          .workspaceId(updatedSource.workspaceId)

      return checkSourceConnectionFromSourceCreate(sourceCoreConfig)
    }

    fun checkDestinationConnectionFromDestinationId(destinationIdRequestBody: DestinationIdRequestBody): CheckConnectionRead {
      val destination = destinationService.getDestinationConnection(destinationIdRequestBody.destinationId)
      val destinationDef =
        destinationService.getStandardDestinationDefinition(destination.destinationDefinitionId)
      val destinationVersion =
        actorDefinitionVersionHelper.getDestinationVersion(destinationDef, destination.workspaceId, destination.destinationId)
      val isCustomConnector = destinationDef.custom
      // ResourceRequirements are read from actor definition and can be null; but if it's not null it will
      // have higher priority and overwrite
      // the default settings in WorkerConfig.
      val resourceRequirements =
        getResourceRequirementsForJobType(destinationDef.resourceRequirements, JobTypeResourceLimit.JobType.CHECK_CONNECTION)
      return reportConnectionStatus(
        synchronousSchedulerClient.createDestinationCheckConnectionJob(destination, destinationVersion, isCustomConnector, resourceRequirements),
      )
    }

    fun checkDestinationConnectionFromDestinationCreate(destinationConfig: DestinationCoreConfig): CheckConnectionRead {
      val destDef = destinationService.getStandardDestinationDefinition(destinationConfig.destinationDefinitionId)
      val destinationVersion =
        actorDefinitionVersionHelper.getDestinationVersion(destDef, destinationConfig.workspaceId, destinationConfig.destinationId)
      val partialConfig =
        secretSanitizer.sanitizePartialConfig(
          destinationConfig.workspaceId,
          destinationConfig.connectionConfiguration,
          destinationVersion.spec,
        )
      val isCustomConnector = destDef.custom

      // todo (cgardens) - narrow the struct passed to the client. we are not setting fields that are
      // technically declared as required.
      val destination =
        DestinationConnection()
          .withDestinationId(destinationConfig.destinationId)
          .withDestinationDefinitionId(destinationConfig.destinationDefinitionId)
          .withConfiguration(partialConfig)
          .withWorkspaceId(destinationConfig.workspaceId)
      // ResourceRequirements are read from actor definition and can be null; but if it's not null it will
      // have higher priority and overwrite
      // the default settings in WorkerConfig.
      val resourceRequirements =
        getResourceRequirementsForJobType(destDef.resourceRequirements, JobTypeResourceLimit.JobType.CHECK_CONNECTION)

      return reportConnectionStatus(
        synchronousSchedulerClient.createDestinationCheckConnectionJob(destination, destinationVersion, isCustomConnector, resourceRequirements),
      )
    }

    fun checkDestinationConnectionFromDestinationIdForUpdate(destinationUpdate: DestinationUpdate): CheckConnectionRead {
      val updatedDestination =
        configurationUpdate
          .destination(destinationUpdate.destinationId, destinationUpdate.name, destinationUpdate.connectionConfiguration)

      val destinationDef =
        destinationService.getStandardDestinationDefinition(updatedDestination.destinationDefinitionId)
      val destinationVersion =
        actorDefinitionVersionHelper.getDestinationVersion(
          destinationDef,
          updatedDestination.workspaceId,
          updatedDestination.destinationId,
        )

      // validate the provided updated config
      jsonSchemaValidator.ensure(
        destinationVersion.spec.connectionSpecification,
        destinationUpdate.connectionConfiguration,
      )

      val destinationCoreConfig =
        DestinationCoreConfig()
          .destinationId(updatedDestination.destinationId)
          .connectionConfiguration(updatedDestination.configuration)
          .destinationDefinitionId(updatedDestination.destinationDefinitionId)
          .workspaceId(updatedDestination.workspaceId)

      return checkDestinationConnectionFromDestinationCreate(destinationCoreConfig)
    }

    fun discoverSchemaForSourceFromSourceId(req: SourceDiscoverSchemaRequestBody): SourceDiscoverSchemaRead {
      val source = sourceService.getSourceConnection(req.sourceId)

      return discover(req, source)
    }

    /**
     * Runs discover schema and does not disable other connections.
     */
    fun discover(
      req: SourceDiscoverSchemaRequestBody,
      source: SourceConnection,
    ): SourceDiscoverSchemaRead {
      val workspace = workspaceService.getStandardWorkspaceNoSecrets(source.workspaceId, includeTombstone = true)
      if (workspace.tombstone) {
        throw NotFoundException("Cannot run discover on tombstoned workspace")
      }

      val sourceId = req.sourceId
      val sourceDef = sourceService.getStandardSourceDefinition(source.sourceDefinitionId)
      val sourceVersion = actorDefinitionVersionHelper.getSourceVersion(sourceDef, source.workspaceId, sourceId)

      val skipCacheCheck = req.disableCache != null && req.disableCache
      // Skip cache check and run discover.
      if (skipCacheCheck) {
        return runDiscoverJobDiffAndConditionallyDisable(source, sourceDef, sourceVersion, req.priority, req.connectionId)
      }

      // Check cache.
      val configHash = Jsons.serialize(source.configuration).toByteArray(StandardCharsets.UTF_8).md5()
      val connectorVersion = sourceVersion.dockerImageTag

      val existingCatalog =
        catalogService.getActorCatalog(req.sourceId, connectorVersion, configHash)

      // No catalog exists, run discover.
      if (existingCatalog.isEmpty) {
        return runDiscoverJobDiffAndConditionallyDisable(source, sourceDef, sourceVersion, req.priority, req.connectionId)
      }

      // We have a catalog cached, no need to run discover. Return cached catalog.
      val airbyteCatalog =
        Jsons.`object`(
          existingCatalog.get().catalog,
          AirbyteCatalog::class.java,
        )
      val emptyJob =
        SynchronousJobRead()
          .configId("NoConfiguration")
          .configType(JobConfigType.DISCOVER_SCHEMA)
          .id(UUID.randomUUID())
          .createdAt(0L)
          .endedAt(0L)
          .logType(LogFormatType.FORMATTED)
          .logs(AttemptInfoReadLogs().logLines(listOf()))
          .succeeded(true)
      return SourceDiscoverSchemaRead()
        .catalog(catalogConverter.toApi(airbyteCatalog, sourceVersion))
        .jobInfo(emptyJob)
        .catalogId(existingCatalog.get().id)
    }

    private fun runDiscoverJobDiffAndConditionallyDisable(
      source: SourceConnection,
      sourceDef: StandardSourceDefinition,
      sourceVersion: ActorDefinitionVersion,
      priority: WorkloadPriority?,
      connectionId: UUID?,
    ): SourceDiscoverSchemaRead {
      val isCustomConnector = sourceDef.custom
      // ResourceRequirements are read from actor definition and can be null; but if it's not null it will
      // have higher priority and overwrite
      // the default settings in WorkerConfig.
      val resourceRequirements =
        getResourceRequirementsForJobType(sourceDef.resourceRequirements, JobTypeResourceLimit.JobType.DISCOVER_SCHEMA)

      val persistedCatalogId: SynchronousResponse<UUID> =
        synchronousSchedulerClient.createDiscoverSchemaJob(
          source,
          sourceVersion,
          isCustomConnector,
          resourceRequirements,
          if (priority == null) io.airbyte.config.WorkloadPriority.HIGH else fromValue(priority.toString()),
        )

      val schemaRead = retrieveDiscoveredSchema(persistedCatalogId, sourceVersion)
      // no connection to diff
      if (connectionId == null || schemaRead.catalogId == null) {
        return schemaRead
      }

      return connectionsHandler.diffCatalogAndConditionallyDisable(connectionId, schemaRead.catalogId)
    }

    fun applySchemaChangeForSource(sourceAutoPropagateChange: SourceAutoPropagateChange) {
      log.info {
        "Applying schema changes for source '${sourceAutoPropagateChange.sourceId}' in workspace '${sourceAutoPropagateChange.workspaceId}'"
      }
      if (sourceAutoPropagateChange.sourceId == null) {
        log.warn { "Missing required field sourceId for applying schema change." }
        return
      }

      if (sourceAutoPropagateChange.workspaceId == null ||
        sourceAutoPropagateChange.catalogId == null ||
        sourceAutoPropagateChange.catalog == null
      ) {
        metricClient.count(
          OssMetricsRegistry.MISSING_APPLY_SCHEMA_CHANGE_INPUT,
          1L,
          MetricAttribute(MetricTags.SOURCE_ID, sourceAutoPropagateChange.sourceId.toString()),
        )
        log.warn {
          "Missing required fields for applying schema change. sourceId: ${sourceAutoPropagateChange.sourceId}, workspaceId: ${sourceAutoPropagateChange.workspaceId}, catalogId: ${sourceAutoPropagateChange.catalogId}, catalog: ${sourceAutoPropagateChange.catalog}"
        }
        return
      }

      val workspace = workspaceService.getStandardWorkspaceNoSecrets(sourceAutoPropagateChange.workspaceId, true)
      val connectionsForSource =
        connectionsHandler.listConnectionsForSource(sourceAutoPropagateChange.sourceId, false)
      for (connectionRead in connectionsForSource.connections) {
        connectionsHandler.applySchemaChange(
          connectionRead.connectionId,
          workspace.workspaceId,
          sourceAutoPropagateChange.catalogId,
          sourceAutoPropagateChange.catalog,
          true,
        )
      }
    }

    fun discoverSchemaForSourceFromSourceCreate(sourceCreate: SourceCoreConfig): SourceDiscoverSchemaRead {
      val workspace = workspaceService.getStandardWorkspaceNoSecrets(sourceCreate.workspaceId, includeTombstone = true)
      if (workspace.tombstone) {
        throw NotFoundException("Cannot run discover on tombstoned workspace")
      }

      val sourceDef = sourceService.getStandardSourceDefinition(sourceCreate.sourceDefinitionId)
      val sourceVersion =
        actorDefinitionVersionHelper.getSourceVersion(sourceDef, sourceCreate.workspaceId, sourceCreate.sourceId)
      val partialConfig =
        secretSanitizer.sanitizePartialConfig(
          sourceCreate.workspaceId,
          sourceCreate.connectionConfiguration,
          sourceVersion.spec,
        )

      val isCustomConnector = sourceDef.custom
      // ResourceRequirements are read from actor definition and can be null; but if it's not null it will
      // have higher priority and overwrite
      // the default settings in WorkerConfig.
      val resourceRequirements =
        getResourceRequirementsForJobType(sourceDef.resourceRequirements, JobTypeResourceLimit.JobType.DISCOVER_SCHEMA)
      // todo (cgardens) - narrow the struct passed to the client. we are not setting fields that are
      // technically declared as required.
      val source =
        SourceConnection()
          .withSourceDefinitionId(sourceCreate.sourceDefinitionId)
          .withConfiguration(partialConfig)
          .withWorkspaceId(sourceCreate.workspaceId)
      val response: SynchronousResponse<UUID> =
        synchronousSchedulerClient.createDiscoverSchemaJob(
          source,
          sourceVersion,
          isCustomConnector,
          resourceRequirements,
          io.airbyte.config.WorkloadPriority.HIGH,
        )
      return retrieveDiscoveredSchema(response, sourceVersion)
    }

    private fun retrieveDiscoveredSchema(
      response: SynchronousResponse<UUID>,
      sourceVersion: ActorDefinitionVersion,
    ): SourceDiscoverSchemaRead {
      val sourceDiscoverSchemaRead =
        SourceDiscoverSchemaRead()
          .jobInfo(jobConverter.getSynchronousJobRead(response))

      if (response.isSuccess) {
        val catalog = catalogService.getActorCatalogById(response.output)
        val persistenceCatalog =
          Jsons.`object`(
            catalog.catalog,
            AirbyteCatalog::class.java,
          )
        sourceDiscoverSchemaRead.catalog(catalogConverter.toApi(persistenceCatalog, sourceVersion))
        sourceDiscoverSchemaRead.catalogId(response.output)
      }

      return sourceDiscoverSchemaRead
    }

    fun syncConnection(connectionIdRequestBody: ConnectionIdRequestBody): JobInfoRead = submitManualSyncToWorker(connectionIdRequestBody.connectionId)

    fun resetConnection(connectionIdRequestBody: ConnectionIdRequestBody): JobInfoRead =
      submitResetConnectionToWorker(connectionIdRequestBody.connectionId)

    fun resetConnectionStream(connectionStreamRequestBody: ConnectionStreamRequestBody): JobInfoRead =
      submitResetConnectionStreamsToWorker(connectionStreamRequestBody.connectionId, connectionStreamRequestBody.streams)

    fun createJob(jobCreate: JobCreate): JobInfoRead {
      // Fail non-terminal jobs first to prevent failing to create a new job
      jobCreationAndStatusUpdateHelper.failNonTerminalJobs(jobCreate.connectionId)

      val standardSync = connectionService.getStandardSync(jobCreate.connectionId)
      val streamsToReset = streamResetPersistence.getStreamResets(jobCreate.connectionId)
      log.info { "Found the following streams to reset for connection ${jobCreate.connectionId}: $streamsToReset" }
      val streamsToRefresh = streamRefreshesHandler.getRefreshesForConnection(jobCreate.connectionId)

      if (!streamsToReset.isEmpty()) {
        val destination = destinationService.getDestinationConnection(standardSync.destinationId)

        val destinationConfiguration =
          oAuthConfigSupplier.injectDestinationOAuthParameters(
            destination.destinationDefinitionId,
            destination.destinationId,
            destination.workspaceId,
            destination.configuration,
          )
        destination.configuration = destinationConfiguration

        val destinationDef =
          destinationService.getStandardDestinationDefinition(destination.destinationDefinitionId)
        val destinationVersion =
          actorDefinitionVersionHelper.getDestinationVersion(destinationDef, destination.workspaceId, destination.destinationId)
        val destinationImageName = destinationVersion.dockerRepository + ":" + destinationVersion.dockerImageTag

        val jobIdOptional =
          jobCreator.createResetConnectionJob(
            destination,
            standardSync,
            destinationDef,
            destinationVersion,
            destinationImageName,
            Version(destinationVersion.protocolVersion),
            destinationDef.custom,
            listOf(),
            streamsToReset,
            destination.workspaceId,
          )

        val jobId =
          if (jobIdOptional.isEmpty) {
            jobPersistence.getLastReplicationJob(standardSync.connectionId).orElseThrow { RuntimeException("No job available") }.id
          } else {
            jobIdOptional.get()
          }

        return jobConverter.getJobInfoRead(jobPersistence.getJob(jobId))
      } else if (!streamsToRefresh.isEmpty()) {
        val jobId = jobFactory.createRefresh(jobCreate.connectionId, streamsToRefresh)

        log.info { "New refresh job created, with id: $jobId" }
        val job = jobPersistence.getJob(jobId)
        jobCreationAndStatusUpdateHelper.emitJobToReleaseStagesMetric(OssMetricsRegistry.JOB_CREATED_BY_RELEASE_STAGE, job)

        return jobConverter.getJobInfoRead(jobPersistence.getJob(jobId))
      } else {
        val jobId = jobFactory.createSync(jobCreate.connectionId, jobCreate.isScheduled)

        log.info { "New job created, with id: $jobId" }
        val job = jobPersistence.getJob(jobId)
        jobCreationAndStatusUpdateHelper.emitJobToReleaseStagesMetric(OssMetricsRegistry.JOB_CREATED_BY_RELEASE_STAGE, job)

        return jobConverter.getJobInfoRead(jobPersistence.getJob(jobId))
      }
    }

    fun cancelJob(jobIdRequestBody: JobIdRequestBody): JobInfoRead {
      log.info { "Canceling job ${jobIdRequestBody.id}" }
      return submitCancellationToWorker(jobIdRequestBody.id)
    }

    private fun reportConnectionStatus(response: SynchronousResponse<StandardCheckConnectionOutput>): CheckConnectionRead {
      val checkConnectionRead =
        CheckConnectionRead()
          .jobInfo(jobConverter.getSynchronousJobRead(response))

      if (response.isSuccess) {
        checkConnectionRead
          .status(
            response.output.status.convertTo<CheckConnectionRead.StatusEnum>(),
          ).message(response.output.message)
      }

      return checkConnectionRead
    }

    private fun submitCancellationToWorker(jobId: Long): JobInfoRead {
      val job = jobPersistence.getJob(jobId)

      val cancellationResult = eventRunner.startNewCancellation(UUID.fromString(job.scope))
      log.info { "Cancellation result for job $jobId: failingReason=${cancellationResult.failingReason} errorCode=${cancellationResult.errorCode}" }
      check(cancellationResult.failingReason == null) { cancellationResult.failingReason!! }
      // log connection timeline event (job cancellation).
      val attemptStats: MutableList<JobPersistence.AttemptStats> = ArrayList()
      for (attempt in job.attempts) {
        attemptStats.add(jobPersistence.getAttemptStats(jobId, attempt.attemptNumber))
      }
      log.info { "Adding connection timeline event for job $jobId attemptStats=$attemptStats" }
      connectionTimelineEventHelper.logJobCancellationEventInConnectionTimeline(job, attemptStats)
      // query same job ID again to get updated job info after cancellation
      return jobConverter.getJobInfoRead(jobPersistence.getJob(jobId))
    }

    private fun submitManualSyncToWorker(connectionId: UUID): JobInfoRead {
      // get standard sync to validate connection id before submitting sync to temporal
      val sync = connectionService.getStandardSync(connectionId)
      check(sync.status == StandardSync.Status.ACTIVE) { "Can only sync an active connection" }
      val manualSyncResult = eventRunner.startNewManualSync(connectionId)
      val jobInfo = readJobFromResult(manualSyncResult)
      connectionTimelineEventHelper.logManuallyStartedEventInConnectionTimeline(connectionId, jobInfo, null)
      return jobInfo
    }

    private fun submitResetConnectionToWorker(
      connectionId: UUID,
      streamsToReset: List<StreamDescriptor> = connectionService.getAllStreamsForConnection(connectionId),
    ): JobInfoRead {
      val resetConnectionResult =
        eventRunner.resetConnection(
          connectionId,
          streamsToReset,
        )

      val jobInfo = readJobFromResult(resetConnectionResult)
      connectionTimelineEventHelper.logManuallyStartedEventInConnectionTimeline(connectionId, jobInfo, streamsToReset)
      return jobInfo
    }

    private fun submitResetConnectionStreamsToWorker(
      connectionId: UUID,
      streams: List<ConnectionStream>,
    ): JobInfoRead {
      val actualStreamsToReset =
        if (streams.isEmpty()) {
          connectionService.getAllStreamsForConnection(connectionId)
        } else {
          streams.stream().map { s: ConnectionStream -> StreamDescriptor().withName(s.streamName).withNamespace(s.streamNamespace) }.toList()
        }
      return submitResetConnectionToWorker(connectionId, actualStreamsToReset)
    }

    fun readJobFromResult(manualOperationResult: ManualOperationResult): JobInfoRead {
      if (manualOperationResult.failingReason != null) {
        if (VALUE_CONFLICT_EXCEPTION_ERROR_CODE_SET.contains(manualOperationResult.errorCode)) {
          throw ValueConflictKnownException(manualOperationResult.failingReason)
        } else {
          throw IllegalStateException(manualOperationResult.failingReason)
        }
      }

      val job = jobPersistence.getJob(manualOperationResult.jobId!!)

      return jobConverter.getJobInfoRead(job)
    }

    companion object {
      private val VALUE_CONFLICT_EXCEPTION_ERROR_CODE_SET: Set<ErrorCode?> = setOf(ErrorCode.WORKFLOW_DELETED, ErrorCode.WORKFLOW_RUNNING)
    }
  }
