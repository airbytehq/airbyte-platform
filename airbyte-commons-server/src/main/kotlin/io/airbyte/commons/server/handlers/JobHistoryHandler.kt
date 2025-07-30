/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.google.common.base.Preconditions
import datadog.trace.api.Trace
import io.airbyte.api.model.generated.AttemptInfoRead
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
import io.airbyte.api.model.generated.StreamStats
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
import io.airbyte.validation.json.JsonValidationException
import io.micronaut.core.util.CollectionUtils
import jakarta.inject.Singleton
import jakarta.validation.Valid
import java.io.IOException
import java.util.Locale
import java.util.Optional
import java.util.SortedMap
import java.util.TreeMap
import java.util.UUID
import java.util.function.Function
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

  @Trace
  @Throws(IOException::class)
  fun listJobsFor(request: JobListRequestBody): JobReadList {
    Preconditions.checkNotNull(request.configTypes, "configType cannot be null.")
    Preconditions.checkState(!request.configTypes.isEmpty(), "Must include at least one configType.")

    val configTypes =
      request.configTypes
        .stream()
        .map { type: JobConfigType ->
          type.convertTo<ConfigType>()
        }.collect(Collectors.toSet())

    val configId = request.configId

    val pageSize =
      if (request.pagination != null && request.pagination.pageSize != null) {
        request.pagination.pageSize
      } else {
        DEFAULT_PAGE_SIZE
      }

    val tags: MutableMap<String?, Any?> = HashMap(java.util.Map.of(MetricTags.CONFIG_TYPES, configTypes.toString()))
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

  @Throws(IOException::class)
  fun listJobsForLight(request: JobListRequestBody): JobReadList {
    Preconditions.checkNotNull(request.configTypes, "configType cannot be null.")
    Preconditions.checkState(!request.configTypes.isEmpty(), "Must include at least one configType.")

    val configTypes =
      request.configTypes
        .stream()
        .map { type: JobConfigType ->
          type.convertTo<ConfigType>()
        }.collect(Collectors.toSet())

    val configId = request.configId

    val pageSize =
      if (request.pagination != null && request.pagination.pageSize != null) {
        request.pagination.pageSize
      } else {
        DEFAULT_PAGE_SIZE
      }

    val tags: MutableMap<String?, Any?> = HashMap(java.util.Map.of(MetricTags.CONFIG_TYPES, configTypes.toString()))
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

  @Throws(IOException::class)
  fun listJobsForWorkspaces(request: JobListForWorkspacesRequestBody): JobReadList {
    Preconditions.checkNotNull(request.configTypes, "configType cannot be null.")
    Preconditions.checkState(!request.configTypes.isEmpty(), "Must include at least one configType.")

    val configTypes =
      request.configTypes
        .stream()
        .map { type: JobConfigType ->
          type.convertTo<ConfigType>()
        }.collect(Collectors.toSet())

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

  @Throws(IOException::class)
  fun getJob(jobId: Long): Job = jobPersistence.getJob(jobId)

  @Throws(IOException::class)
  fun getJobInfo(jobId: Long): JobInfoRead {
    val job = jobPersistence.getJob(jobId)
    return jobConverter.getJobInfoRead(job)
  }

  @Throws(IOException::class)
  fun getJobInfoWithoutLogs(jobId: Long): JobInfoRead {
    val job = jobPersistence.getJob(jobId)

    val jobWithAttemptsRead = getJobWithAttemptsRead(job)
    hydrateWithStats(java.util.List.of(jobWithAttemptsRead), java.util.List.of(job), true, jobPersistence)

    return JobInfoRead()
      .job(jobWithAttemptsRead.job)
      .attempts(
        job.attempts
          .stream()
          .map { obj: Attempt -> JobConverter.getAttemptInfoWithoutLogsRead(obj) }
          .collect(Collectors.toList<@Valid AttemptInfoRead?>()),
      )
  }

  @Throws(IOException::class)
  fun getJobInfoLight(jobIdRequestBody: JobIdRequestBody): JobInfoLightRead {
    val job = jobPersistence.getJob(jobIdRequestBody.id)
    return jobConverter.getJobInfoLightRead(job)
  }

  @Throws(IOException::class)
  fun getLastReplicationJob(connectionIdRequestBody: ConnectionIdRequestBody): JobOptionalRead {
    val job = jobPersistence.getLastReplicationJob(connectionIdRequestBody.connectionId)
    return jobConverter.getJobOptionalRead(job)
  }

  @Throws(IOException::class)
  fun getLastReplicationJobWithCancel(connectionIdRequestBody: ConnectionIdRequestBody): JobOptionalRead {
    val job = jobPersistence.getLastReplicationJobWithCancel(connectionIdRequestBody.connectionId)
    return jobConverter.getJobOptionalRead(job)
  }

  @Throws(
    io.airbyte.config.persistence.ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
  )
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

  @Trace
  @Throws(IOException::class)
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

  @Throws(IOException::class)
  fun getConnectionSyncProgress(connectionIdRequestBody: ConnectionIdRequestBody): ConnectionSyncProgressRead {
    val jobs = jobPersistence.getRunningJobForConnection(connectionIdRequestBody.connectionId)

    val jobReads =
      jobs
        .stream()
        .map { obj: Job -> getJobWithAttemptsRead(obj) }
        .collect(Collectors.toList())

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
      runningJob
        .job.streamAggregatedStats
        .stream()
        .collect(
          Collectors.toMap<@Valid StreamStats?, String, StreamStats?>(
            { streamStats: StreamStats? -> streamStats!!.streamName + "-" + streamStats.streamNamespace },
            Function.identity<@Valid StreamStats?>(),
          ),
        )

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

  @Trace
  @Throws(IOException::class)
  fun getLatestSyncJob(connectionId: UUID): Optional<JobRead> =
    jobPersistence.getLastSyncJob(connectionId).map { obj: Job -> JobConverter.getJobRead(obj) }

  @Trace
  @Throws(IOException::class)
  fun getLatestSyncJobsForConnections(connectionIds: List<UUID>): List<JobStatusSummary> = jobPersistence.getLastSyncJobForConnections(connectionIds)

  @Trace
  @Throws(IOException::class)
  fun getRunningSyncJobForConnections(connectionIds: List<UUID>): List<JobRead> =
    jobPersistence
      .getRunningSyncJobForConnections(connectionIds)
      .stream()
      .map { obj: Job -> JobConverter.getJobRead(obj) }
      .collect(Collectors.toList())

  @Throws(
    JsonValidationException::class,
    IOException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
    ConfigNotFoundException::class,
  )
  private fun getSourceRead(connectionRead: ConnectionRead): SourceRead {
    val sourceIdRequestBody = SourceIdRequestBody().sourceId(connectionRead.sourceId)
    return sourceHandler.getSource(sourceIdRequestBody)
  }

  @Throws(
    JsonValidationException::class,
    IOException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
    ConfigNotFoundException::class,
  )
  private fun getDestinationRead(connectionRead: ConnectionRead): DestinationRead {
    val destinationIdRequestBody = DestinationIdRequestBody().destinationId(connectionRead.destinationId)
    return destinationHandler.getDestination(destinationIdRequestBody)
  }

  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  private fun getSourceDefinitionRead(sourceRead: SourceRead): SourceDefinitionRead =
    sourceDefinitionsHandler.getSourceDefinition(sourceRead.sourceDefinitionId, true)

  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  private fun getDestinationDefinitionRead(destinationRead: DestinationRead): DestinationDefinitionRead =
    destinationDefinitionsHandler.getDestinationDefinition(destinationRead.destinationDefinitionId, true)

  @Throws(
    io.airbyte.config.persistence.ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
  )
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
      .stream()
      .map { apiJobStatus: JobStatus ->
        io.airbyte.config.JobStatus
          .valueOf(apiJobStatus.toString().uppercase(Locale.getDefault()))
      }.collect(Collectors.toList())

  @JvmRecord
  data class StreamNameAndNamespace(
    val name: String,
    val namespace: String?,
  )

  companion object {
    const val DEFAULT_PAGE_SIZE: Int = 200

    fun getStreamsToSyncMode(job: Job): Map<StreamNameAndNamespace, SyncMode> {
      val configuredAirbyteStreams = extractStreams(job)
      return configuredAirbyteStreams
        .stream()
        .collect(
          Collectors.toMap(
            { configuredStream: ConfiguredAirbyteStream ->
              StreamNameAndNamespace(
                configuredStream.stream.name,
                configuredStream.stream.namespace,
              )
            },
            ConfiguredAirbyteStream::syncMode,
          ),
        )
    }

    private fun extractStreams(job: Job): List<ConfiguredAirbyteStream> {
      val configuredCatalog = JobConfigProxy(job.config).configuredCatalog
      return configuredCatalog?.streams ?: listOf()
    }
  }
}
