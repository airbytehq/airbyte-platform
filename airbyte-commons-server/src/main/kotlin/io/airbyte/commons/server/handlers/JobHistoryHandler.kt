/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionSyncProgressRead
import io.airbyte.api.model.generated.DestinationDefinitionRead
import io.airbyte.api.model.generated.DestinationIdRequestBody
import io.airbyte.api.model.generated.DestinationRead
import io.airbyte.api.model.generated.JobConfigType
import io.airbyte.api.model.generated.JobDebugInfoRead
import io.airbyte.api.model.generated.JobIdRequestBody
import io.airbyte.api.model.generated.JobInfoLightRead
import io.airbyte.api.model.generated.JobInfoRead
import io.airbyte.api.model.generated.JobListForWorkspacesRequestBody
import io.airbyte.api.model.generated.JobListRequestBody
import io.airbyte.api.model.generated.JobOptionalRead
import io.airbyte.api.model.generated.JobRead
import io.airbyte.api.model.generated.JobReadList
import io.airbyte.api.model.generated.JobStatus
import io.airbyte.api.model.generated.JobWithAttemptsRead
import io.airbyte.api.model.generated.SourceDefinitionRead
import io.airbyte.api.model.generated.SourceIdRequestBody
import io.airbyte.api.model.generated.SourceRead
import io.airbyte.api.model.generated.StreamDescriptor
import io.airbyte.api.model.generated.StreamSyncProgressReadItem
import io.airbyte.api.model.generated.WorkflowStateRead
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.converters.JobConverter
import io.airbyte.commons.server.converters.JobConverter.Companion.getDebugJobInfoRead
import io.airbyte.commons.server.converters.JobConverter.Companion.getJobWithAttemptsRead
import io.airbyte.commons.server.converters.WorkflowStateConverter
import io.airbyte.commons.server.handlers.helpers.StatsAggregationHelper.hydrateWithStats
import io.airbyte.commons.temporal.TemporalClient
import io.airbyte.commons.temporal.scheduling.state.WorkflowState
import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.config.Attempt
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.Job
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobConfigProxy
import io.airbyte.config.JobStatusSummary
import io.airbyte.config.StandardSync
import io.airbyte.config.SyncMode
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.JobService
import io.airbyte.featureflag.ANONYMOUS
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.HydrateAggregatedStats
import io.airbyte.featureflag.Workspace
import io.airbyte.metrics.lib.ApmTraceUtils.addTagsToTrace
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.persistence.job.JobPersistence
import io.micronaut.core.util.CollectionUtils
import io.opentelemetry.instrumentation.annotations.WithSpan
import jakarta.inject.Singleton
import jakarta.validation.Valid
import java.util.Locale
import java.util.Optional
import java.util.SortedMap
import java.util.TreeMap
import java.util.UUID
import java.util.stream.Collectors

/**
 * JobHistoryHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
class JobHistoryHandler(
  private val jobPersistence: JobPersistence,
  private val connectionService: ConnectionService,
  private val sourceHandler: SourceHandler,
  private val sourceDefinitionsHandler: SourceDefinitionsHandler,
  private val destinationHandler: DestinationHandler,
  private val destinationDefinitionsHandler: DestinationDefinitionsHandler,
  private val airbyteVersion: AirbyteVersion,
  private val temporalClient: TemporalClient?,
  private val featureFlagClient: FeatureFlagClient,
  private val jobConverter: JobConverter,
  private val jobService: JobService,
  private val apiPojoConverters: ApiPojoConverters,
) {
  private val workflowStateConverter = WorkflowStateConverter()

  @WithSpan
  fun listJobsFor(request: JobListRequestBody): JobReadList {
    requireNotNull(request.configTypes) { "configType cannot be null." }
    check(!request.configTypes.isEmpty()) { "Must include at least one configType." }

    val configTypes =
      request.configTypes
        .map { type: JobConfigType ->
          type.convertTo<ConfigType>()
        }.toSet()

    val configId = request.configId

    val pageSize =
      if (request.pagination != null && request.pagination.pageSize != null) {
        request.pagination.pageSize
      } else {
        DEFAULT_PAGE_SIZE
      }

    val tags: MutableMap<String?, Any?> = HashMap(mapOf(MetricTags.CONFIG_TYPES to configTypes.toString()))
    if (configId != null) {
      tags[MetricTags.CONNECTION_ID] = configId
    }
    addTagsToTrace(tags)

    val jobs: List<Job> =
      if (request.includingJobId != null) {
        jobPersistence.listJobsIncludingId(
          configTypes,
          configId,
          request.includingJobId,
          pageSize,
        )
      } else {
        jobService.listJobs(
          configTypes,
          configId,
          pageSize,
          if (request.pagination != null && request.pagination.rowOffset != null) request.pagination.rowOffset else 0,
          if (CollectionUtils.isEmpty(request.statuses)) emptyList() else mapToDomainJobStatus(request.statuses),
          request.createdAtStart,
          request.createdAtEnd,
          request.updatedAtStart,
          request.updatedAtEnd,
          if (request.orderByField == null) null else request.orderByField.value(),
          if (request.orderByMethod == null) null else request.orderByMethod.value(),
        )
      }

    val jobReads = jobs.map { obj: Job -> getJobWithAttemptsRead(obj) }

    hydrateWithStats(
      jobReads,
      jobs,
      featureFlagClient.boolVariation(HydrateAggregatedStats, Workspace(ANONYMOUS)),
      jobPersistence,
    )

    val totalJobCount =
      jobPersistence.getJobCount(
        configTypes,
        configId,
        if (CollectionUtils.isEmpty(request.statuses)) null else mapToDomainJobStatus(request.statuses),
        request.createdAtStart,
        request.createdAtEnd,
        request.updatedAtStart,
        request.updatedAtEnd,
      )
    return JobReadList().jobs(jobReads).totalJobCount(totalJobCount)
  }

  fun listJobsForLight(request: JobListRequestBody): JobReadList {
    requireNotNull(request.configTypes) { "configType cannot be null." }
    check(!request.configTypes.isEmpty()) { "Must include at least one configType." }

    val configTypes =
      request.configTypes
        .map { type: JobConfigType ->
          type.convertTo<ConfigType>()
        }.toSet()

    val configId = request.configId

    val pageSize =
      if (request.pagination != null && request.pagination.pageSize != null) {
        request.pagination.pageSize
      } else {
        DEFAULT_PAGE_SIZE
      }

    val tags: MutableMap<String?, Any?> = hashMapOf(MetricTags.CONFIG_TYPES to configTypes.toString())
    if (configId != null) {
      tags[MetricTags.CONNECTION_ID] = configId
    }
    addTagsToTrace(tags)

    val jobs: List<Job> =
      if (request.includingJobId != null) {
        jobPersistence.listJobsIncludingId(
          configTypes,
          configId,
          request.includingJobId,
          pageSize,
        )
      } else {
        jobPersistence.listJobsLight(
          configTypes,
          configId,
          pageSize,
          if (request.pagination != null && request.pagination.rowOffset != null) request.pagination.rowOffset else 0,
          if (CollectionUtils.isEmpty(request.statuses)) null else mapToDomainJobStatus(request.statuses),
          request.createdAtStart,
          request.createdAtEnd,
          request.updatedAtStart,
          request.updatedAtEnd,
          if (request.orderByField == null) null else request.orderByField.value(),
          if (request.orderByMethod == null) null else request.orderByMethod.value(),
        )
      }

    val jobReads = jobs.stream().map { obj: Job -> getJobWithAttemptsRead(obj) }.collect(Collectors.toList())

    hydrateWithStats(
      jobReads,
      jobs,
      featureFlagClient.boolVariation(HydrateAggregatedStats, Workspace(ANONYMOUS)),
      jobPersistence,
    )

    val totalJobCount =
      jobPersistence.getJobCount(
        configTypes,
        configId,
        if (CollectionUtils.isEmpty(request.statuses)) null else mapToDomainJobStatus(request.statuses),
        request.createdAtStart,
        request.createdAtEnd,
        request.updatedAtStart,
        request.updatedAtEnd,
      )
    return JobReadList().jobs(jobReads).totalJobCount(totalJobCount)
  }

  fun listJobsForWorkspaces(request: JobListForWorkspacesRequestBody): JobReadList {
    requireNotNull(request.configTypes) { "configType cannot be null." }
    check(!request.configTypes.isEmpty()) { "Must include at least one configType." }

    val configTypes =
      request.configTypes
        .map { type: JobConfigType ->
          type.convertTo<ConfigType>()
        }.toSet()

    val pageSize =
      if (request.pagination != null && request.pagination.pageSize != null) {
        request.pagination.pageSize
      } else {
        DEFAULT_PAGE_SIZE
      }

    val offset =
      if (request.pagination != null && request.pagination.rowOffset != null) request.pagination.rowOffset else 0

    val jobs =
      jobPersistence.listJobsLight(
        configTypes,
        request.workspaceIds,
        pageSize,
        offset,
        if (CollectionUtils.isEmpty(request.statuses)) null else mapToDomainJobStatus(request.statuses),
        request.createdAtStart,
        request.createdAtEnd,
        request.updatedAtStart,
        request.updatedAtEnd,
        if (request.orderByField == null) null else request.orderByField.value(),
        if (request.orderByMethod == null) null else request.orderByMethod.value(),
      )

    val jobReads = jobs.stream().map { obj: Job -> getJobWithAttemptsRead(obj) }.collect(Collectors.toList())

    hydrateWithStats(
      jobReads,
      jobs,
      featureFlagClient.boolVariation(HydrateAggregatedStats, Workspace(ANONYMOUS)),
      jobPersistence,
    )

    return JobReadList().jobs(jobReads).totalJobCount(jobs.size.toLong())
  }

  fun getJob(jobId: Long): Job = jobPersistence.getJob(jobId)

  fun getJobInfo(jobId: Long): JobInfoRead {
    val job = jobPersistence.getJob(jobId)
    return jobConverter.getJobInfoRead(job)
  }

  fun getJobInfoWithoutLogs(jobId: Long): JobInfoRead = getJobInfoWithoutLogsStatic(jobPersistence, jobId)

  fun getJobInfoLight(jobIdRequestBody: JobIdRequestBody): JobInfoLightRead {
    val job = jobPersistence.getJob(jobIdRequestBody.id)
    return jobConverter.getJobInfoLightRead(job)
  }

  fun getLastReplicationJob(connectionIdRequestBody: ConnectionIdRequestBody): JobOptionalRead {
    val job = jobPersistence.getLastReplicationJob(connectionIdRequestBody.connectionId)
    return jobConverter.getJobOptionalRead(job)
  }

  fun getLastReplicationJobWithCancel(connectionIdRequestBody: ConnectionIdRequestBody): JobOptionalRead {
    val job = jobPersistence.getLastReplicationJobWithCancel(connectionIdRequestBody.connectionId)
    return jobConverter.getJobOptionalRead(job)
  }

  fun getJobDebugInfo(jobId: Long): JobDebugInfoRead {
    val job = jobPersistence.getJob(jobId)
    val jobinfoRead = jobConverter.getJobInfoRead(job)

    for (a in jobinfoRead.attempts) {
      val attemptNumber = a.attempt.id.toInt()
      val attemptStats = jobPersistence.getAttemptStats(job.id, attemptNumber)
      hydrateWithStats(a.attempt, attemptStats)
    }

    val jobDebugInfoRead = buildJobDebugInfoRead(jobinfoRead)
    if (temporalClient != null) {
      val connectionId = UUID.fromString(job.scope)
      Optional
        .ofNullable(temporalClient.getWorkflowState(connectionId))
        .map { workflowState: WorkflowState? ->
          workflowStateConverter.getWorkflowStateRead(
            workflowState!!,
          )
        }.ifPresent { workflowState: WorkflowStateRead? ->
          jobDebugInfoRead.workflowState =
            workflowState
        }
    }

    return jobDebugInfoRead
  }

  @WithSpan
  fun getLatestRunningSyncJob(connectionId: UUID): Optional<JobRead> {
    val nonTerminalSyncJobsForConnection =
      jobPersistence.listJobsForConnectionWithStatuses(
        connectionId,
        Job.SYNC_REPLICATION_TYPES,
        io.airbyte.config.JobStatus.NON_TERMINAL_STATUSES,
      )

    // there *should* only be a single running sync job for a connection, but
    // jobPersistence.listJobsForConnectionWithStatuses orders by created_at desc so
    // .findFirst will always return what we want.
    return nonTerminalSyncJobsForConnection.stream().map { obj: Job -> JobConverter.getJobRead(obj) }.findFirst()
  }

  fun getConnectionSyncProgress(connectionIdRequestBody: ConnectionIdRequestBody): ConnectionSyncProgressRead {
    val jobs = jobPersistence.getRunningJobForConnection(connectionIdRequestBody.connectionId)

    val jobReads =
      jobs
        .map { obj: Job -> getJobWithAttemptsRead(obj) }

    return getConnectionSyncProgressInternal(connectionIdRequestBody, jobs, jobReads)
  }

  fun getConnectionSyncProgressInternal(
    connectionIdRequestBody: ConnectionIdRequestBody,
    jobs: List<Job>,
    jobReads: List<JobWithAttemptsRead>,
  ): ConnectionSyncProgressRead {
    hydrateWithStats(jobReads, jobs, featureFlagClient.boolVariation(HydrateAggregatedStats, Workspace(ANONYMOUS)), jobPersistence)

    if (jobReads.isEmpty() || jobReads.first() == null) {
      return ConnectionSyncProgressRead()
        .connectionId(connectionIdRequestBody.connectionId)
        .streams(emptyList<@Valid StreamSyncProgressReadItem?>())
    }
    val runningJob: JobWithAttemptsRead = jobReads.first()

    // Create a map from the stream stats list
    val streamStatsMap =
      runningJob.job.streamAggregatedStats.associateBy { streamStats -> "${streamStats.streamName}-${streamStats.streamNamespace}" }

    // Iterate through ALL enabled streams from the job, enriching with stream stats data
    val runningJobConfigType = runningJob.job.configType
    val streamToTrackPerConfigType: SortedMap<JobConfigType, List<StreamDescriptor>> = TreeMap()
    val enabledStreams = runningJob.job.enabledStreams
    if (runningJobConfigType == JobConfigType.SYNC) {
      streamToTrackPerConfigType[JobConfigType.SYNC] = enabledStreams
    } else if (runningJobConfigType == JobConfigType.REFRESH) {
      val streamsToRefresh = runningJob.job.refreshConfig.streamsToRefresh
      streamToTrackPerConfigType[JobConfigType.REFRESH] = streamsToRefresh
      streamToTrackPerConfigType[JobConfigType.SYNC] =
        enabledStreams.stream().filter { s: StreamDescriptor -> !streamsToRefresh.contains(s) }.toList()
    } else if (runningJobConfigType == JobConfigType.RESET_CONNECTION || runningJobConfigType == JobConfigType.CLEAR) {
      streamToTrackPerConfigType[runningJobConfigType] = runningJob.job.resetConfig.streamsToReset
    }

    val finalStreamsWithStats =
      streamToTrackPerConfigType.entries
        .stream()
        .flatMap { entry: Map.Entry<JobConfigType, List<StreamDescriptor>> ->
          entry.value.stream().map { stream: StreamDescriptor ->
            val key = stream.name + "-" + stream.namespace
            val streamStats = streamStatsMap[key]

            val item =
              StreamSyncProgressReadItem()
                .streamName(stream.name)
                .streamNamespace(stream.namespace)
                .configType(entry.key)

            if (streamStats != null) {
              item
                .recordsEmitted(streamStats.recordsEmitted)
                .recordsCommitted(streamStats.recordsCommitted)
                .bytesEmitted(streamStats.bytesEmitted)
                .bytesCommitted(streamStats.bytesCommitted)
                .recordsRejected(streamStats.recordsRejected)
            }
            item
          }
        }.collect(Collectors.toList())

    val aggregatedStats = runningJob.job.aggregatedStats
    return ConnectionSyncProgressRead()
      .connectionId(connectionIdRequestBody.connectionId)
      .jobId(runningJob.job.id)
      .syncStartedAt(runningJob.job.createdAt)
      .bytesEmitted(aggregatedStats?.bytesEmitted)
      .bytesCommitted(aggregatedStats?.bytesCommitted)
      .recordsEmitted(aggregatedStats?.recordsEmitted)
      .recordsCommitted(aggregatedStats?.recordsCommitted)
      .recordsRejected(aggregatedStats?.recordsRejected)
      .configType(runningJobConfigType)
      .streams(finalStreamsWithStats)
  }

  @WithSpan
  fun getLatestSyncJob(connectionId: UUID): Optional<JobRead> =
    jobPersistence.getLastSyncJob(connectionId).map { obj: Job -> JobConverter.getJobRead(obj) }

  @WithSpan
  fun getLatestSyncJobsForConnections(connectionIds: List<UUID>): List<JobStatusSummary> = jobPersistence.getLastSyncJobForConnections(connectionIds)

  @WithSpan
  fun getRunningSyncJobForConnections(connectionIds: List<UUID>): List<JobRead> =
    jobPersistence
      .getRunningSyncJobForConnections(connectionIds)
      .map { obj: Job -> JobConverter.getJobRead(obj) }

  private fun getSourceRead(connectionRead: ConnectionRead): SourceRead {
    val sourceIdRequestBody = SourceIdRequestBody().sourceId(connectionRead.sourceId)
    return sourceHandler.getSource(sourceIdRequestBody)
  }

  private fun getDestinationRead(connectionRead: ConnectionRead): DestinationRead {
    val destinationIdRequestBody = DestinationIdRequestBody().destinationId(connectionRead.destinationId)
    return destinationHandler.getDestination(destinationIdRequestBody)
  }

  private fun getSourceDefinitionRead(sourceRead: SourceRead): SourceDefinitionRead =
    sourceDefinitionsHandler.getSourceDefinition(sourceRead.sourceDefinitionId, true)

  private fun getDestinationDefinitionRead(destinationRead: DestinationRead): DestinationDefinitionRead =
    destinationDefinitionsHandler.getDestinationDefinition(destinationRead.destinationDefinitionId, true)

  private fun buildJobDebugInfoRead(jobInfoRead: JobInfoRead): JobDebugInfoRead {
    val configId = jobInfoRead.job.configId
    val standardSync: StandardSync
    try {
      standardSync = connectionService.getStandardSync(UUID.fromString(configId))
    } catch (e: ConfigNotFoundException) {
      throw io.airbyte.config.persistence
        .ConfigNotFoundException(e.type, e.message)
    }

    val connection = apiPojoConverters.internalToConnectionRead(standardSync)
    val source = getSourceRead(connection)
    val destination = getDestinationRead(connection)
    val sourceDefinitionRead = getSourceDefinitionRead(source)
    val destinationDefinitionRead = getDestinationDefinitionRead(destination)
    val jobDebugRead = getDebugJobInfoRead(jobInfoRead, sourceDefinitionRead, destinationDefinitionRead, airbyteVersion)

    return JobDebugInfoRead()
      .attempts(jobInfoRead.attempts)
      .job(jobDebugRead)
  }

  fun mapToDomainJobStatus(apiJobStatuses: List<JobStatus>): List<io.airbyte.config.JobStatus> =
    apiJobStatuses
      .map { apiJobStatus: JobStatus ->
        io.airbyte.config.JobStatus
          .valueOf(apiJobStatus.toString().uppercase(Locale.getDefault()))
      }

  data class StreamNameAndNamespace(
    val name: String,
    val namespace: String?,
  )

  companion object {
    const val DEFAULT_PAGE_SIZE: Int = 200

    fun getStreamsToSyncMode(job: Job): Map<StreamNameAndNamespace, SyncMode> {
      val configuredAirbyteStreams = extractStreams(job)
      return configuredAirbyteStreams
        .associate { configuredStream ->
          StreamNameAndNamespace(
            configuredStream.stream.name,
            configuredStream.stream.namespace,
          ) to configuredStream.syncMode
        }
    }

    private fun extractStreams(job: Job): List<ConfiguredAirbyteStream> {
      val configuredCatalog = JobConfigProxy(job.config).configuredCatalog
      return configuredCatalog?.streams ?: listOf()
    }

    fun getJobInfoWithoutLogsStatic(
      jobPersistence: JobPersistence,
      jobId: Long,
    ): JobInfoRead {
      val job = jobPersistence.getJob(jobId)

      val jobWithAttemptsRead = getJobWithAttemptsRead(job)
      hydrateWithStats(listOf(jobWithAttemptsRead), listOf(job), true, jobPersistence)

      return JobInfoRead()
        .job(jobWithAttemptsRead.job)
        .attempts(
          job.attempts
            .map { obj: Attempt -> JobConverter.getAttemptInfoWithoutLogsRead(obj) },
        )
    }
  }
}
