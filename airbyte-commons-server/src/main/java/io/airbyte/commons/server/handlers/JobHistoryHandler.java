/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.commons.server.handlers.helpers.StatsAggregationHelper.hydrateWithStats;
import static io.airbyte.config.Job.SYNC_REPLICATION_TYPES;
import static io.airbyte.featureflag.ContextKt.ANONYMOUS;

import com.google.common.base.Preconditions;
import datadog.trace.api.Trace;
import io.airbyte.api.model.generated.AttemptInfoRead;
import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.api.model.generated.ConnectionSyncProgressRead;
import io.airbyte.api.model.generated.DestinationDefinitionRead;
import io.airbyte.api.model.generated.DestinationIdRequestBody;
import io.airbyte.api.model.generated.DestinationRead;
import io.airbyte.api.model.generated.JobAggregatedStats;
import io.airbyte.api.model.generated.JobConfigType;
import io.airbyte.api.model.generated.JobDebugInfoRead;
import io.airbyte.api.model.generated.JobDebugRead;
import io.airbyte.api.model.generated.JobIdRequestBody;
import io.airbyte.api.model.generated.JobInfoLightRead;
import io.airbyte.api.model.generated.JobInfoRead;
import io.airbyte.api.model.generated.JobListForWorkspacesRequestBody;
import io.airbyte.api.model.generated.JobListRequestBody;
import io.airbyte.api.model.generated.JobOptionalRead;
import io.airbyte.api.model.generated.JobRead;
import io.airbyte.api.model.generated.JobReadList;
import io.airbyte.api.model.generated.JobWithAttemptsRead;
import io.airbyte.api.model.generated.SourceDefinitionRead;
import io.airbyte.api.model.generated.SourceIdRequestBody;
import io.airbyte.api.model.generated.SourceRead;
import io.airbyte.api.model.generated.StreamDescriptor;
import io.airbyte.api.model.generated.StreamStats;
import io.airbyte.api.model.generated.StreamSyncProgressReadItem;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.converters.JobConverter;
import io.airbyte.commons.server.converters.WorkflowStateConverter;
import io.airbyte.commons.temporal.TemporalClient;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.Job;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.JobConfigProxy;
import io.airbyte.config.JobStatus;
import io.airbyte.config.JobStatusSummary;
import io.airbyte.config.StandardSync;
import io.airbyte.config.SyncMode;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.JobService;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.HydrateAggregatedStats;
import io.airbyte.featureflag.Workspace;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.validation.json.JsonValidationException;
import io.micronaut.core.util.CollectionUtils;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * JobHistoryHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
@SuppressWarnings("PMD.PreserveStackTrace")
public class JobHistoryHandler {

  private final ConnectionService connectionService;
  private final SourceHandler sourceHandler;
  private final DestinationHandler destinationHandler;
  private final SourceDefinitionsHandler sourceDefinitionsHandler;
  private final DestinationDefinitionsHandler destinationDefinitionsHandler;
  public static final int DEFAULT_PAGE_SIZE = 200;
  private final JobPersistence jobPersistence;
  private final JobConverter jobConverter;
  private final WorkflowStateConverter workflowStateConverter;
  private final AirbyteVersion airbyteVersion;
  private final TemporalClient temporalClient;
  private final FeatureFlagClient featureFlagClient;
  private final JobService jobService;
  private final ApiPojoConverters apiPojoConverters;

  public JobHistoryHandler(final JobPersistence jobPersistence,
                           final ConnectionService connectionService,
                           final SourceHandler sourceHandler,
                           final SourceDefinitionsHandler sourceDefinitionsHandler,
                           final DestinationHandler destinationHandler,
                           final DestinationDefinitionsHandler destinationDefinitionsHandler,
                           final AirbyteVersion airbyteVersion,
                           final TemporalClient temporalClient,
                           final FeatureFlagClient featureFlagClient,
                           final JobConverter jobConverter,
                           final JobService jobService,
                           final ApiPojoConverters apiPojoConverters) {
    this.featureFlagClient = featureFlagClient;
    this.jobConverter = jobConverter;
    this.jobService = jobService;
    workflowStateConverter = new WorkflowStateConverter();
    this.jobPersistence = jobPersistence;
    this.connectionService = connectionService;
    this.sourceHandler = sourceHandler;
    this.sourceDefinitionsHandler = sourceDefinitionsHandler;
    this.destinationHandler = destinationHandler;
    this.destinationDefinitionsHandler = destinationDefinitionsHandler;
    this.airbyteVersion = airbyteVersion;
    this.temporalClient = temporalClient;
    this.apiPojoConverters = apiPojoConverters;
  }

  @SuppressWarnings("UnstableApiUsage")
  @Trace
  public JobReadList listJobsFor(final JobListRequestBody request) throws IOException {
    Preconditions.checkNotNull(request.getConfigTypes(), "configType cannot be null.");
    Preconditions.checkState(!request.getConfigTypes().isEmpty(), "Must include at least one configType.");

    final Set<ConfigType> configTypes = request.getConfigTypes()
        .stream()
        .map(type -> Enums.convertTo(type, JobConfig.ConfigType.class))
        .collect(Collectors.toSet());

    final String configId = request.getConfigId();

    final int pageSize = (request.getPagination() != null && request.getPagination().getPageSize() != null) ? request.getPagination().getPageSize()
        : DEFAULT_PAGE_SIZE;
    final List<Job> jobs;

    final Map<String, Object> tags = new HashMap<>(Map.of(MetricTags.CONFIG_TYPES, configTypes.toString()));
    if (configId != null) {
      tags.put(MetricTags.CONNECTION_ID, configId);
    }
    ApmTraceUtils.addTagsToTrace(tags);

    if (request.getIncludingJobId() != null) {
      jobs = jobPersistence.listJobsIncludingId(
          configTypes,
          configId,
          request.getIncludingJobId(),
          pageSize);
    } else {
      jobs = jobService.listJobs(configTypes, configId, pageSize,
          (request.getPagination() != null && request.getPagination().getRowOffset() != null) ? request.getPagination().getRowOffset() : 0,
          CollectionUtils.isEmpty(request.getStatuses()) ? Collections.emptyList() : mapToDomainJobStatus(request.getStatuses()),
          request.getCreatedAtStart(),
          request.getCreatedAtEnd(),
          request.getUpdatedAtStart(),
          request.getUpdatedAtEnd(),
          request.getOrderByField() == null ? null : request.getOrderByField().value(),
          request.getOrderByMethod() == null ? null : request.getOrderByMethod().value());
    }

    final List<JobWithAttemptsRead> jobReads = jobs.stream().map(JobConverter::getJobWithAttemptsRead).collect(Collectors.toList());

    hydrateWithStats(
        jobReads,
        jobs,
        featureFlagClient.boolVariation(HydrateAggregatedStats.INSTANCE, new Workspace(ANONYMOUS)),
        jobPersistence);

    final Long totalJobCount = jobPersistence.getJobCount(configTypes, configId,
        CollectionUtils.isEmpty(request.getStatuses()) ? null : mapToDomainJobStatus(request.getStatuses()),
        request.getCreatedAtStart(),
        request.getCreatedAtEnd(),
        request.getUpdatedAtStart(),
        request.getUpdatedAtEnd());
    return new JobReadList().jobs(jobReads).totalJobCount(totalJobCount);
  }

  public JobReadList listJobsForLight(final JobListRequestBody request) throws IOException {
    Preconditions.checkNotNull(request.getConfigTypes(), "configType cannot be null.");
    Preconditions.checkState(!request.getConfigTypes().isEmpty(), "Must include at least one configType.");

    final Set<ConfigType> configTypes = request.getConfigTypes()
        .stream()
        .map(type -> Enums.convertTo(type, JobConfig.ConfigType.class))
        .collect(Collectors.toSet());

    final String configId = request.getConfigId();

    final int pageSize = (request.getPagination() != null && request.getPagination().getPageSize() != null) ? request.getPagination().getPageSize()
        : DEFAULT_PAGE_SIZE;
    final List<Job> jobs;

    final Map<String, Object> tags = new HashMap<>(Map.of(MetricTags.CONFIG_TYPES, configTypes.toString()));
    if (configId != null) {
      tags.put(MetricTags.CONNECTION_ID, configId);
    }
    ApmTraceUtils.addTagsToTrace(tags);

    if (request.getIncludingJobId() != null) {
      jobs = jobPersistence.listJobsIncludingId(
          configTypes,
          configId,
          request.getIncludingJobId(),
          pageSize);
    } else {
      jobs = jobPersistence.listJobsLight(configTypes, configId, pageSize,
          (request.getPagination() != null && request.getPagination().getRowOffset() != null) ? request.getPagination().getRowOffset() : 0,
          CollectionUtils.isEmpty(request.getStatuses()) ? null : mapToDomainJobStatus(request.getStatuses()),
          request.getCreatedAtStart(),
          request.getCreatedAtEnd(),
          request.getUpdatedAtStart(),
          request.getUpdatedAtEnd(),
          request.getOrderByField() == null ? null : request.getOrderByField().value(),
          request.getOrderByMethod() == null ? null : request.getOrderByMethod().value());
    }

    final List<JobWithAttemptsRead> jobReads = jobs.stream().map(JobConverter::getJobWithAttemptsRead).collect(Collectors.toList());

    hydrateWithStats(
        jobReads,
        jobs,
        featureFlagClient.boolVariation(HydrateAggregatedStats.INSTANCE, new Workspace(ANONYMOUS)),
        jobPersistence);

    final Long totalJobCount = jobPersistence.getJobCount(configTypes, configId,
        CollectionUtils.isEmpty(request.getStatuses()) ? null : mapToDomainJobStatus(request.getStatuses()),
        request.getCreatedAtStart(),
        request.getCreatedAtEnd(),
        request.getUpdatedAtStart(),
        request.getUpdatedAtEnd());
    return new JobReadList().jobs(jobReads).totalJobCount(totalJobCount);
  }

  @SuppressWarnings("UnstableApiUsage")
  public JobReadList listJobsForWorkspaces(final JobListForWorkspacesRequestBody request) throws IOException {

    Preconditions.checkNotNull(request.getConfigTypes(), "configType cannot be null.");
    Preconditions.checkState(!request.getConfigTypes().isEmpty(), "Must include at least one configType.");

    final Set<ConfigType> configTypes = request.getConfigTypes()
        .stream()
        .map(type -> Enums.convertTo(type, JobConfig.ConfigType.class))
        .collect(Collectors.toSet());

    final int pageSize = (request.getPagination() != null && request.getPagination().getPageSize() != null) ? request.getPagination().getPageSize()
        : DEFAULT_PAGE_SIZE;

    final int offset =
        (request.getPagination() != null && request.getPagination().getRowOffset() != null) ? request.getPagination().getRowOffset() : 0;

    final List<Job> jobs = jobPersistence.listJobsLight(
        configTypes,
        request.getWorkspaceIds(),
        pageSize,
        offset,
        CollectionUtils.isEmpty(request.getStatuses()) ? null : mapToDomainJobStatus(request.getStatuses()),
        request.getCreatedAtStart(),
        request.getCreatedAtEnd(),
        request.getUpdatedAtStart(),
        request.getUpdatedAtEnd(),
        request.getOrderByField() == null ? null : request.getOrderByField().value(),
        request.getOrderByMethod() == null ? null : request.getOrderByMethod().value());

    final List<JobWithAttemptsRead> jobReads = jobs.stream().map(JobConverter::getJobWithAttemptsRead).collect(Collectors.toList());

    hydrateWithStats(
        jobReads,
        jobs,
        featureFlagClient.boolVariation(HydrateAggregatedStats.INSTANCE, new Workspace(ANONYMOUS)),
        jobPersistence);

    return new JobReadList().jobs(jobReads).totalJobCount((long) jobs.size());
  }

  public static Map<StreamNameAndNamespace, SyncMode> getStreamsToSyncMode(Job job) {
    List<ConfiguredAirbyteStream> configuredAirbyteStreams = extractStreams(job);
    return configuredAirbyteStreams.stream()
        .collect(Collectors.toMap(
            configuredStream -> new StreamNameAndNamespace(configuredStream.getStream().getName(), configuredStream.getStream().getNamespace()),
            ConfiguredAirbyteStream::getSyncMode));
  }

  private static List<ConfiguredAirbyteStream> extractStreams(Job job) {
    final var configuredCatalog = new JobConfigProxy(job.getConfig()).getConfiguredCatalog();
    return configuredCatalog != null ? configuredCatalog.getStreams() : List.of();
  }

  public Job getJob(final Long jobId) throws IOException {
    return jobPersistence.getJob(jobId);
  }

  public JobInfoRead getJobInfo(final Long jobId) throws IOException {
    final Job job = jobPersistence.getJob(jobId);
    return jobConverter.getJobInfoRead(job);
  }

  public JobInfoRead getJobInfoWithoutLogs(final Long jobId) throws IOException {
    final Job job = jobPersistence.getJob(jobId);

    final JobWithAttemptsRead jobWithAttemptsRead = JobConverter.getJobWithAttemptsRead(job);
    hydrateWithStats(List.of(jobWithAttemptsRead), List.of(job), true, jobPersistence);

    return new JobInfoRead()
        .job(jobWithAttemptsRead.getJob())
        .attempts(job.getAttempts().stream().map(JobConverter::getAttemptInfoWithoutLogsRead).collect(Collectors.toList()));
  }

  public JobInfoLightRead getJobInfoLight(final JobIdRequestBody jobIdRequestBody) throws IOException {
    final Job job = jobPersistence.getJob(jobIdRequestBody.getId());
    return jobConverter.getJobInfoLightRead(job);
  }

  public JobOptionalRead getLastReplicationJob(final ConnectionIdRequestBody connectionIdRequestBody) throws IOException {
    final Optional<Job> job = jobPersistence.getLastReplicationJob(connectionIdRequestBody.getConnectionId());
    return jobConverter.getJobOptionalRead(job);

  }

  public JobOptionalRead getLastReplicationJobWithCancel(final ConnectionIdRequestBody connectionIdRequestBody) throws IOException {
    final Optional<Job> job = jobPersistence.getLastReplicationJobWithCancel(connectionIdRequestBody.getConnectionId());
    return jobConverter.getJobOptionalRead(job);

  }

  public JobDebugInfoRead getJobDebugInfo(final Long jobId)
      throws ConfigNotFoundException, IOException, JsonValidationException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final Job job = jobPersistence.getJob(jobId);
    final JobInfoRead jobinfoRead = jobConverter.getJobInfoRead(job);

    for (final AttemptInfoRead a : jobinfoRead.getAttempts()) {
      final int attemptNumber = a.getAttempt().getId().intValue();
      final var attemptStats = jobPersistence.getAttemptStats(job.getId(), attemptNumber);
      hydrateWithStats(a.getAttempt(), attemptStats);
    }

    final JobDebugInfoRead jobDebugInfoRead = buildJobDebugInfoRead(jobinfoRead);
    if (temporalClient != null) {
      final UUID connectionId = UUID.fromString(job.getScope());
      Optional.ofNullable(temporalClient.getWorkflowState(connectionId))
          .map(workflowStateConverter::getWorkflowStateRead)
          .ifPresent(jobDebugInfoRead::setWorkflowState);
    }

    return jobDebugInfoRead;
  }

  @Trace
  public Optional<JobRead> getLatestRunningSyncJob(final UUID connectionId) throws IOException {
    final List<Job> nonTerminalSyncJobsForConnection = jobPersistence.listJobsForConnectionWithStatuses(
        connectionId,
        SYNC_REPLICATION_TYPES,
        JobStatus.NON_TERMINAL_STATUSES);

    // there *should* only be a single running sync job for a connection, but
    // jobPersistence.listJobsForConnectionWithStatuses orders by created_at desc so
    // .findFirst will always return what we want.
    return nonTerminalSyncJobsForConnection.stream().map(JobConverter::getJobRead).findFirst();
  }

  public ConnectionSyncProgressRead getConnectionSyncProgress(final ConnectionIdRequestBody connectionIdRequestBody) throws IOException {
    final List<Job> jobs = jobPersistence.getRunningJobForConnection(connectionIdRequestBody.getConnectionId());

    final List<JobWithAttemptsRead> jobReads = jobs.stream()
        .map(JobConverter::getJobWithAttemptsRead)
        .collect(Collectors.toList());

    hydrateWithStats(jobReads, jobs, featureFlagClient.boolVariation(HydrateAggregatedStats.INSTANCE, new Workspace(ANONYMOUS)), jobPersistence);

    if (jobReads.isEmpty() || jobReads.getFirst() == null) {
      return new ConnectionSyncProgressRead().connectionId(connectionIdRequestBody.getConnectionId()).streams(Collections.emptyList());
    }
    final JobWithAttemptsRead runningJob = jobReads.getFirst();

    // Create a map from the stream stats list
    final Map<String, StreamStats> streamStatsMap = runningJob
        .getJob().getStreamAggregatedStats().stream()
        .collect(Collectors.toMap(
            streamStats -> streamStats.getStreamName() + "-" + streamStats.getStreamNamespace(),
            Function.identity()));

    // Iterate through ALL enabled streams from the job, enriching with stream stats data
    final JobConfigType runningJobConfigType = runningJob.getJob().getConfigType();
    final SortedMap<JobConfigType, List<StreamDescriptor>> streamToTrackPerConfigType = new TreeMap<>();
    final var enabledStreams = runningJob.getJob().getEnabledStreams();
    if (runningJobConfigType.equals(JobConfigType.SYNC)) {
      streamToTrackPerConfigType.put(JobConfigType.SYNC, enabledStreams);
    } else if (runningJobConfigType.equals(JobConfigType.REFRESH)) {
      final List<StreamDescriptor> streamsToRefresh = runningJob.getJob().getRefreshConfig().getStreamsToRefresh();
      streamToTrackPerConfigType.put(JobConfigType.REFRESH, streamsToRefresh);
      streamToTrackPerConfigType.put(JobConfigType.SYNC, enabledStreams.stream().filter(s -> !streamsToRefresh.contains(s)).toList());
    } else if (runningJobConfigType.equals(JobConfigType.RESET_CONNECTION) || runningJobConfigType.equals(JobConfigType.CLEAR)) {
      streamToTrackPerConfigType.put(runningJobConfigType, runningJob.getJob().getResetConfig().getStreamsToReset());
    }

    final List<StreamSyncProgressReadItem> finalStreamsWithStats = streamToTrackPerConfigType.entrySet().stream()
        .flatMap((entry) -> {
          return entry.getValue().stream().map(stream -> {
            final String key = stream.getName() + "-" + stream.getNamespace();
            final StreamStats streamStats = streamStatsMap.get(key);

            final StreamSyncProgressReadItem item = new StreamSyncProgressReadItem()
                .streamName(stream.getName())
                .streamNamespace(stream.getNamespace())
                .configType(entry.getKey());

            if (streamStats != null) {
              item.recordsEmitted(streamStats.getRecordsEmitted())
                  .recordsCommitted(streamStats.getRecordsCommitted())
                  .bytesEmitted(streamStats.getBytesEmitted())
                  .bytesCommitted(streamStats.getBytesCommitted());
            }

            return item;
          });
        }).collect(Collectors.toList());

    final JobAggregatedStats aggregatedStats = runningJob.getJob().getAggregatedStats();
    return new ConnectionSyncProgressRead()
        .connectionId(connectionIdRequestBody.getConnectionId())
        .jobId(runningJob.getJob().getId())
        .syncStartedAt(runningJob.getJob().getCreatedAt())
        .bytesEmitted(aggregatedStats == null ? null : aggregatedStats.getBytesEmitted())
        .bytesCommitted(aggregatedStats == null ? null : aggregatedStats.getBytesCommitted())
        .recordsEmitted(aggregatedStats == null ? null : aggregatedStats.getRecordsEmitted())
        .recordsCommitted(aggregatedStats == null ? null : aggregatedStats.getRecordsCommitted())
        .configType(runningJobConfigType)
        .streams(finalStreamsWithStats);
  }

  @Trace
  public Optional<JobRead> getLatestSyncJob(final UUID connectionId) throws IOException {
    return jobPersistence.getLastSyncJob(connectionId).map(JobConverter::getJobRead);
  }

  @Trace
  public List<JobStatusSummary> getLatestSyncJobsForConnections(final List<UUID> connectionIds) throws IOException {
    return jobPersistence.getLastSyncJobForConnections(connectionIds);
  }

  @Trace
  public List<JobRead> getRunningSyncJobForConnections(final List<UUID> connectionIds) throws IOException {
    return jobPersistence.getRunningSyncJobForConnections(connectionIds).stream()
        .map(JobConverter::getJobRead)
        .collect(Collectors.toList());
  }

  private SourceRead getSourceRead(final ConnectionRead connectionRead)
      throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final SourceIdRequestBody sourceIdRequestBody = new SourceIdRequestBody().sourceId(connectionRead.getSourceId());
    return sourceHandler.getSource(sourceIdRequestBody);
  }

  private DestinationRead getDestinationRead(final ConnectionRead connectionRead)
      throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final DestinationIdRequestBody destinationIdRequestBody = new DestinationIdRequestBody().destinationId(connectionRead.getDestinationId());
    return destinationHandler.getDestination(destinationIdRequestBody);
  }

  private SourceDefinitionRead getSourceDefinitionRead(final SourceRead sourceRead)
      throws JsonValidationException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    return sourceDefinitionsHandler.getSourceDefinition(sourceRead.getSourceDefinitionId(), true);
  }

  private DestinationDefinitionRead getDestinationDefinitionRead(final DestinationRead destinationRead)
      throws JsonValidationException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {

    return destinationDefinitionsHandler.getDestinationDefinition(destinationRead.getDestinationDefinitionId(), true);
  }

  private JobDebugInfoRead buildJobDebugInfoRead(final JobInfoRead jobInfoRead)
      throws ConfigNotFoundException, IOException, JsonValidationException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final String configId = jobInfoRead.getJob().getConfigId();
    final StandardSync standardSync;
    try {
      standardSync = connectionService.getStandardSync(UUID.fromString(configId));
    } catch (io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getMessage());
    }

    final ConnectionRead connection = apiPojoConverters.internalToConnectionRead(standardSync);
    final SourceRead source = getSourceRead(connection);
    final DestinationRead destination = getDestinationRead(connection);
    final SourceDefinitionRead sourceDefinitionRead = getSourceDefinitionRead(source);
    final DestinationDefinitionRead destinationDefinitionRead = getDestinationDefinitionRead(destination);
    final JobDebugRead jobDebugRead = JobConverter.getDebugJobInfoRead(jobInfoRead, sourceDefinitionRead, destinationDefinitionRead, airbyteVersion);

    return new JobDebugInfoRead()
        .attempts(jobInfoRead.getAttempts())
        .job(jobDebugRead);
  }

  public List<JobStatus> mapToDomainJobStatus(List<io.airbyte.api.model.generated.JobStatus> apiJobStatuses) {
    return apiJobStatuses.stream()
        .map(apiJobStatus -> JobStatus.valueOf(apiJobStatus.toString().toUpperCase()))
        .collect(Collectors.toList());
  }

  public record StreamNameAndNamespace(String name, String namespace) {}

}
