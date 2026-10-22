/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.ConnectionRead
import io.airbyte.api.server.generated.models.ConnectionSyncProgressRead
import io.airbyte.api.server.generated.models.DestinationDefinitionRead
import io.airbyte.api.server.generated.models.DestinationIdRequestBody
import io.airbyte.api.server.generated.models.DestinationRead
import io.airbyte.api.server.generated.models.JobConfigType
import io.airbyte.api.server.generated.models.JobDebugInfoRead
import io.airbyte.api.server.generated.models.JobIdRequestBody
import io.airbyte.api.server.generated.models.JobInfoLightRead
import io.airbyte.api.server.generated.models.JobInfoRead
import io.airbyte.api.server.generated.models.JobListForWorkspacesRequestBody
import io.airbyte.api.server.generated.models.JobListRequestBody
import io.airbyte.api.server.generated.models.JobOptionalRead
import io.airbyte.api.server.generated.models.JobRead
import io.airbyte.api.server.generated.models.JobReadList
import io.airbyte.api.server.generated.models.JobStatus
import io.airbyte.api.server.generated.models.JobWithAttemptsRead
import io.airbyte.api.server.generated.models.SourceDefinitionRead
import io.airbyte.api.server.generated.models.SourceIdRequestBody
import io.airbyte.api.server.generated.models.SourceRead
import io.airbyte.api.server.generated.models.StreamDescriptor
import io.airbyte.api.server.generated.models.StreamSyncProgressReadItem
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.converters.JobConverter
import io.airbyte.commons.server.converters.JobConverter.Companion.getDebugJobInfoRead
import io.airbyte.commons.server.converters.JobConverter.Companion.getJobWithAttemptsRead
import io.airbyte.commons.server.converters.WorkflowStateConverter
import io.airbyte.commons.server.handlers.helpers.StatsAggregationHelper.hydrateWithStats
import io.airbyte.commons.temporal.TemporalClient
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
      request.pagination?.pageSize ?: DEFAULT_PAGE_SIZE

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
          request.includingJobId!!,
          pageSize,
        )
      } else {
        jobService.listJobs(
          configTypes,
          configId,
          pageSize,
          request.pagination?.rowOffset ?: 0,
          if (CollectionUtils.isEmpty(request.statuses)) emptyList() else mapToDomainJobStatus(request.statuses!!),
          request.createdAtStart,
          request.createdAtEnd,
          request.updatedAtStart,
          request.updatedAtEnd,
          request.orderByField?.value,
          request.orderByMethod?.value,
        )
      }

    val jobReads =
      hydrateWithStats(
        jobs.map { obj: Job -> getJobWithAttemptsRead(obj) },
        jobs,
        featureFlagClient.boolVariation(HydrateAggregatedStats, Workspace(ANONYMOUS)),
        jobPersistence,
      )

    val totalJobCount =
      jobPersistence.getJobCount(
        configTypes,
        configId,
        if (CollectionUtils.isEmpty(request.statuses)) null else mapToDomainJobStatus(request.statuses!!),
        request.createdAtStart,
        request.createdAtEnd,
        request.updatedAtStart,
        request.updatedAtEnd,
      )
    return JobReadList(jobs = jobReads, totalJobCount = totalJobCount)
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
      request.pagination?.pageSize ?: DEFAULT_PAGE_SIZE

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
          request.includingJobId!!,
          pageSize,
        )
      } else {
        jobPersistence.listJobsLight(
          configTypes,
          configId,
          pageSize,
          request.pagination?.rowOffset ?: 0,
          if (CollectionUtils.isEmpty(request.statuses)) null else mapToDomainJobStatus(request.statuses!!),
          request.createdAtStart,
          request.createdAtEnd,
          request.updatedAtStart,
          request.updatedAtEnd,
          request.orderByField?.value,
          request.orderByMethod?.value,
        )
      }

    val jobReads =
      hydrateWithStats(
        jobs.stream().map { obj: Job -> getJobWithAttemptsRead(obj) }.collect(Collectors.toList()),
        jobs,
        featureFlagClient.boolVariation(HydrateAggregatedStats, Workspace(ANONYMOUS)),
        jobPersistence,
      )

    val totalJobCount =
      jobPersistence.getJobCount(
        configTypes,
        configId,
        if (CollectionUtils.isEmpty(request.statuses)) null else mapToDomainJobStatus(request.statuses!!),
        request.createdAtStart,
        request.createdAtEnd,
        request.updatedAtStart,
        request.updatedAtEnd,
      )
    return JobReadList(jobs = jobReads, totalJobCount = totalJobCount)
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
      request.pagination?.pageSize ?: DEFAULT_PAGE_SIZE

    val offset =
      request.pagination?.rowOffset ?: 0

    val jobs =
      jobPersistence.listJobsLight(
        configTypes,
        request.workspaceIds!!,
        pageSize,
        offset,
        if (CollectionUtils.isEmpty(request.statuses)) null else mapToDomainJobStatus(request.statuses!!),
        request.createdAtStart,
        request.createdAtEnd,
        request.updatedAtStart,
        request.updatedAtEnd,
        request.orderByField?.value,
        request.orderByMethod?.value,
      )

    val jobReads =
      hydrateWithStats(
        jobs.stream().map { obj: Job -> getJobWithAttemptsRead(obj) }.collect(Collectors.toList()),
        jobs,
        featureFlagClient.boolVariation(HydrateAggregatedStats, Workspace(ANONYMOUS)),
        jobPersistence,
      )

    return JobReadList(jobs = jobReads, totalJobCount = jobs.size.toLong())
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
    val jobInfoRead = jobConverter.getJobInfoRead(job)
    val hydratedJobInfoRead =
      jobInfoRead.copy(
        attempts =
          jobInfoRead.attempts.map { a ->
            val attemptStats = jobPersistence.getAttemptStats(job.id, a.attempt.id.toInt())
            a.copy(attempt = hydrateWithStats(a.attempt, attemptStats))
          },
      )

    val jobDebugInfoRead = buildJobDebugInfoRead(hydratedJobInfoRead)
    if (temporalClient == null) {
      return jobDebugInfoRead
    }
    val workflowState = temporalClient.getWorkflowState(UUID.fromString(job.scope)) ?: return jobDebugInfoRead
    return jobDebugInfoRead.copy(workflowState = workflowStateConverter.getWorkflowStateRead(workflowState))
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
    val hydratedJobReads =
      hydrateWithStats(jobReads, jobs, featureFlagClient.boolVariation(HydrateAggregatedStats, Workspace(ANONYMOUS)), jobPersistence)

    if (hydratedJobReads.isEmpty()) {
      return ConnectionSyncProgressRead(
        connectionId = connectionIdRequestBody.connectionId,
        streams = emptyList(),
      )
    }
    val runningJob = hydratedJobReads.first().job!!

    // Create a map from the stream stats list
    val streamStatsMap =
      runningJob.streamAggregatedStats.orEmpty().associateBy { streamStats -> "${streamStats.streamName}-${streamStats.streamNamespace}" }

    // Iterate through ALL enabled streams from the job, enriching with stream stats data
    val runningJobConfigType = runningJob.configType
    val streamToTrackPerConfigType: SortedMap<JobConfigType, List<StreamDescriptor>> = TreeMap()
    val enabledStreams = runningJob.enabledStreams.orEmpty()
    if (runningJobConfigType == JobConfigType.SYNC) {
      streamToTrackPerConfigType[JobConfigType.SYNC] = enabledStreams
    } else if (runningJobConfigType == JobConfigType.REFRESH) {
      val streamsToRefresh = runningJob.refreshConfig!!.streamsToRefresh.orEmpty()
      streamToTrackPerConfigType[JobConfigType.REFRESH] = streamsToRefresh
      streamToTrackPerConfigType[JobConfigType.SYNC] =
        enabledStreams.stream().filter { s: StreamDescriptor -> !streamsToRefresh.contains(s) }.toList()
    } else if (runningJobConfigType == JobConfigType.RESET_CONNECTION || runningJobConfigType == JobConfigType.CLEAR) {
      streamToTrackPerConfigType[runningJobConfigType] = runningJob.resetConfig!!.streamsToReset.orEmpty()
    }

    val finalStreamsWithStats =
      streamToTrackPerConfigType.entries
        .stream()
        .flatMap { entry: Map.Entry<JobConfigType, List<StreamDescriptor>> ->
          entry.value.stream().map { stream: StreamDescriptor ->
            val key = stream.name + "-" + stream.namespace
            val streamStats = streamStatsMap[key]

            StreamSyncProgressReadItem(
              streamName = stream.name,
              streamNamespace = stream.namespace,
              configType = entry.key,
              recordsEmitted = streamStats?.recordsEmitted,
              recordsCommitted = streamStats?.recordsCommitted,
              bytesEmitted = streamStats?.bytesEmitted,
              bytesCommitted = streamStats?.bytesCommitted,
              recordsRejected = streamStats?.recordsRejected,
            )
          }
        }.collect(Collectors.toList())

    val aggregatedStats = runningJob.aggregatedStats
    return ConnectionSyncProgressRead(
      connectionId = connectionIdRequestBody.connectionId,
      jobId = runningJob.id,
      syncStartedAt = runningJob.createdAt,
      bytesEmitted = aggregatedStats?.bytesEmitted,
      bytesCommitted = aggregatedStats?.bytesCommitted,
      recordsEmitted = aggregatedStats?.recordsEmitted,
      recordsCommitted = aggregatedStats?.recordsCommitted,
      recordsRejected = aggregatedStats?.recordsRejected,
      configType = runningJobConfigType,
      streams = finalStreamsWithStats,
    )
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
    val sourceIdRequestBody = SourceIdRequestBody(sourceId = connectionRead.sourceId)
    return sourceHandler.getSource(sourceIdRequestBody)
  }

  private fun getDestinationRead(connectionRead: ConnectionRead): DestinationRead {
    val destinationIdRequestBody = DestinationIdRequestBody(destinationId = connectionRead.destinationId)
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

    return JobDebugInfoRead(attempts = jobInfoRead.attempts, job = jobDebugRead)
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
    ): JobInfoRead = getJobInfoWithoutLogsStatic(jobPersistence, jobPersistence.getJob(jobId))

    fun getJobInfoWithoutLogsStatic(
      jobPersistence: JobPersistence,
      job: Job,
    ): JobInfoRead {
      val jobWithAttemptsRead = hydrateWithStats(listOf(getJobWithAttemptsRead(job)), listOf(job), true, jobPersistence).single()

      return JobInfoRead(
        job = jobWithAttemptsRead.job!!,
        attempts =
          job.attempts
            .map { obj: Attempt -> JobConverter.getAttemptInfoWithoutLogsRead(obj) },
      )
    }
  }
}
