/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers;

import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.ConnectionSyncResultRead;
import io.airbyte.api.model.generated.ConnectionUptimeHistoryRequestBody;
import io.airbyte.api.model.generated.JobAggregatedStats;
import io.airbyte.api.model.generated.JobRead;
import io.airbyte.api.model.generated.JobStatus;
import io.airbyte.api.model.generated.JobSyncResultRead;
import io.airbyte.api.model.generated.JobWithAttemptsRead;
import io.airbyte.api.model.generated.StreamStatusCreateRequestBody;
import io.airbyte.api.model.generated.StreamStatusIncompleteRunCause;
import io.airbyte.api.model.generated.StreamStatusListRequestBody;
import io.airbyte.api.model.generated.StreamStatusRead;
import io.airbyte.api.model.generated.StreamStatusReadList;
import io.airbyte.api.model.generated.StreamStatusRunState;
import io.airbyte.api.model.generated.StreamStatusUpdateRequestBody;
import io.airbyte.commons.server.handlers.JobHistoryHandler;
import io.airbyte.commons.server.handlers.helpers.StatsAggregationHelper;
import io.airbyte.config.Job;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.server.handlers.api_domain_mapping.StreamStatusesMapper;
import io.airbyte.server.repositories.StreamStatusesRepository;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Interface layer between the API and Persistence layers.
 */
@Singleton
public class StreamStatusesHandler {

  final StreamStatusesRepository repo;
  final StreamStatusesMapper mapper;
  private final JobHistoryHandler jobHistoryHandler;
  private final JobPersistence jobPersistence;

  public StreamStatusesHandler(final StreamStatusesRepository repo,
                               final StreamStatusesMapper mapper,
                               final JobHistoryHandler jobHistoryHandler,
                               JobPersistence jobPersistence) {
    this.repo = repo;
    this.mapper = mapper;
    this.jobHistoryHandler = jobHistoryHandler;
    this.jobPersistence = jobPersistence;
  }

  public StreamStatusRead createStreamStatus(final StreamStatusCreateRequestBody req) {
    final var model = mapper.map(req);

    final var saved = repo.save(model);

    return mapper.map(saved);
  }

  public StreamStatusRead updateStreamStatus(final StreamStatusUpdateRequestBody req) {
    final var model = mapper.map(req);

    final var saved = repo.update(model);

    return mapper.map(saved);
  }

  public StreamStatusReadList listStreamStatus(final StreamStatusListRequestBody req) {
    final var filters = mapper.map(req);

    final var page = repo.findAllFiltered(filters);

    final var apiList = page.getContent()
        .stream()
        .map(mapper::map)
        .toList();

    return new StreamStatusReadList().streamStatuses(apiList);
  }

  public StreamStatusReadList listStreamStatusPerRunState(final ConnectionIdRequestBody req) {
    final var apiList = repo.findAllPerRunStateByConnectionId(req.getConnectionId())
        .stream()
        .map(mapper::map)
        .toList();

    return new StreamStatusReadList().streamStatuses(apiList);
  }

  public ConnectionSyncResultRead mapStreamStatusToSyncReadResult(final StreamStatusRead streamStatus) {
    final JobStatus jobStatus = streamStatus.getRunState() == StreamStatusRunState.COMPLETE ? JobStatus.SUCCEEDED
        : streamStatus.getIncompleteRunCause() == StreamStatusIncompleteRunCause.CANCELED ? JobStatus.CANCELLED : JobStatus.FAILED;

    final ConnectionSyncResultRead result = new ConnectionSyncResultRead();
    result.setStatus(jobStatus);
    result.setStreamName(streamStatus.getStreamName());
    result.setStreamNamespace(streamStatus.getStreamNamespace());
    return result;
  }

  /**
   * Get the uptime history for a specific connection over the last X jobs.
   *
   * @param req the request body
   * @return list of JobSyncResultReads.
   */
  public List<JobSyncResultRead> getConnectionUptimeHistory(final ConnectionUptimeHistoryRequestBody req) {

    final List<StreamStatusRead> streamStatuses = repo.findLastAttemptsOfLastXJobsForConnection(req.getConnectionId(), req.getNumberOfJobs())
        .stream()
        .map(mapper::map)
        .toList();

    final Map<Long, List<StreamStatusRead>> jobIdToStreamStatuses =
        streamStatuses.stream().collect(Collectors.groupingBy(StreamStatusRead::getJobId));

    final List<JobSyncResultRead> result = new ArrayList<>();

    List<Job> jobs;
    try {
      jobs = jobPersistence.listJobsLight(new HashSet<>(jobIdToStreamStatuses.keySet()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Map<Long, JobWithAttemptsRead> jobIdToJobRead = StatsAggregationHelper.getJobIdToJobWithAttemptsReadMap(jobs, jobPersistence);

    jobIdToStreamStatuses.forEach((jobId, statuses) -> {
      final JobRead job = jobIdToJobRead.get(jobId).getJob();
      final JobAggregatedStats aggregatedStats = job.getAggregatedStats();
      final JobSyncResultRead jobResult = new JobSyncResultRead()
          .jobId(jobId)
          .configType(job.getConfigType())
          .jobCreatedAt(job.getCreatedAt())
          .jobUpdatedAt(job.getUpdatedAt())
          .streamStatuses(statuses.stream().map(this::mapStreamStatusToSyncReadResult).toList())
          .bytesEmitted(aggregatedStats.getBytesEmitted())
          .bytesCommitted(aggregatedStats.getBytesCommitted())
          .recordsEmitted(aggregatedStats.getRecordsEmitted())
          .recordsCommitted(aggregatedStats.getRecordsCommitted());
      result.add(jobResult);
    });

    return result;
  }

}
