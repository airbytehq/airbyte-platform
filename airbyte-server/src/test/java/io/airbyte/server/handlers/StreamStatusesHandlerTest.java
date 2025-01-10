/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers;

import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.ConnectionSyncResultRead;
import io.airbyte.api.model.generated.ConnectionUptimeHistoryRequestBody;
import io.airbyte.api.model.generated.JobAggregatedStats;
import io.airbyte.api.model.generated.JobConfigType;
import io.airbyte.api.model.generated.JobRead;
import io.airbyte.api.model.generated.JobSyncResultRead;
import io.airbyte.api.model.generated.JobWithAttemptsRead;
import io.airbyte.api.model.generated.StreamStatusCreateRequestBody;
import io.airbyte.api.model.generated.StreamStatusListRequestBody;
import io.airbyte.api.model.generated.StreamStatusRead;
import io.airbyte.api.model.generated.StreamStatusReadList;
import io.airbyte.api.model.generated.StreamStatusUpdateRequestBody;
import io.airbyte.commons.server.handlers.JobHistoryHandler;
import io.airbyte.commons.server.handlers.helpers.StatsAggregationHelper;
import io.airbyte.config.Job;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.JobStatus;
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusJobType;
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusRunState;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.server.handlers.api_domain_mapping.StreamStatusesMapper;
import io.airbyte.server.repositories.StreamStatusesRepository;
import io.airbyte.server.repositories.StreamStatusesRepository.FilterParams;
import io.airbyte.server.repositories.domain.StreamStatus;
import io.micronaut.data.model.Page;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class StreamStatusesHandlerTest {

  StreamStatusesRepository repo;

  StreamStatusesMapper mapper;

  StreamStatusesHandler handler;
  JobPersistence jobPersistence;
  JobHistoryHandler jobHistoryHandler;

  @BeforeEach
  void setup() {
    repo = Mockito.mock(StreamStatusesRepository.class);
    mapper = Mockito.mock(StreamStatusesMapper.class);
    jobHistoryHandler = Mockito.mock(JobHistoryHandler.class);
    jobPersistence = Mockito.mock(JobPersistence.class);
    handler = new StreamStatusesHandler(repo, mapper, jobHistoryHandler, jobPersistence);
  }

  @Test
  void testCreate() {
    final var apiReq = new StreamStatusCreateRequestBody();
    final var domain = new StreamStatus.StreamStatusBuilder().build();
    final var apiResp = new StreamStatusRead();

    when(mapper.map(apiReq))
        .thenReturn(domain);
    when(repo.save(domain))
        .thenReturn(domain);
    when(mapper.map(domain))
        .thenReturn(apiResp);

    Assertions.assertSame(apiResp, handler.createStreamStatus(apiReq));
  }

  @Test
  void testUpdate() {
    final var apiReq = new StreamStatusUpdateRequestBody();
    final var domain = new StreamStatus.StreamStatusBuilder().build();
    final var apiResp = new StreamStatusRead();

    when(mapper.map(apiReq))
        .thenReturn(domain);
    when(repo.update(domain))
        .thenReturn(domain);
    when(mapper.map(domain))
        .thenReturn(apiResp);

    Assertions.assertSame(apiResp, handler.updateStreamStatus(apiReq));
  }

  @Test
  void testList() {
    final var apiReq = new StreamStatusListRequestBody();
    final var domainFilters = new FilterParams(null, null, null, null, null, null, null, null);
    final var domainItem = new StreamStatus.StreamStatusBuilder().build();
    final var apiItem = new StreamStatusRead();
    final var apiResp = new StreamStatusReadList().streamStatuses(List.of(apiItem));

    when(mapper.map(Mockito.any(StreamStatusListRequestBody.class)))
        .thenReturn(domainFilters);

    final var page = Mockito.mock(Page.class);
    when(page.getContent()).thenReturn(List.of(domainItem));
    when(repo.findAllFiltered(domainFilters))
        .thenReturn(page);
    when(mapper.map(domainItem))
        .thenReturn(apiItem);

    Assertions.assertEquals(apiResp, handler.listStreamStatus(apiReq));
  }

  @Test
  void testListPerRunState() {
    final var connectionId = UUID.randomUUID();
    final var apiReq = new ConnectionIdRequestBody().connectionId(connectionId);
    final var domainItem = new StreamStatus.StreamStatusBuilder().build();
    final var apiItem = new StreamStatusRead();
    final var apiResp = new StreamStatusReadList().streamStatuses(List.of(apiItem));

    when(repo.findAllPerRunStateByConnectionId(connectionId))
        .thenReturn(List.of(domainItem));
    when(mapper.map(domainItem))
        .thenReturn(apiItem);

    Assertions.assertEquals(apiResp, handler.listStreamStatusPerRunState(apiReq));
  }

  @Test
  void testUptimeHistory() throws IOException {
    final var connectionId = UUID.randomUUID();
    final var numJobs = 10;
    final ConnectionUptimeHistoryRequestBody apiReq = new ConnectionUptimeHistoryRequestBody().numberOfJobs(numJobs).connectionId(connectionId);
    final long jobOneId = 1L;
    final StreamStatus ssOne = new StreamStatus.StreamStatusBuilder()
        .id(UUID.randomUUID())
        .connectionId(connectionId)
        .attemptNumber(0)
        .streamName("streamOne")
        .streamNamespace("streamOneNamespace")
        .jobId(jobOneId)
        .jobType(JobStreamStatusJobType.sync)
        .transitionedAt(OffsetDateTime.now())
        .runState(JobStreamStatusRunState.complete).build();
    final StreamStatus ssTwo = new StreamStatus.StreamStatusBuilder()
        .id(UUID.randomUUID())
        .connectionId(connectionId)
        .attemptNumber(0)
        .streamName("streamTwo")
        .streamNamespace("streamTwoNamespace")
        .jobId(jobOneId)
        .jobType(JobStreamStatusJobType.sync)
        .transitionedAt(OffsetDateTime.now())
        .runState(JobStreamStatusRunState.complete).build();
    final long jobTwoId = 2L;
    final StreamStatus ssThree = new StreamStatus.StreamStatusBuilder()
        .id(UUID.randomUUID())
        .connectionId(connectionId)
        .attemptNumber(0)
        .streamName("streamThree")
        .streamNamespace("streamThreeNamespace")
        .jobId(jobTwoId)
        .jobType(JobStreamStatusJobType.sync)
        .transitionedAt(OffsetDateTime.now())
        .runState(JobStreamStatusRunState.complete).build();

    final Job jobOne = new Job(1, ConfigType.SYNC, connectionId.toString(), null, List.of(), JobStatus.SUCCEEDED, 0L, 0L, 0L);
    final Job jobTwo = new Job(2, ConfigType.SYNC, connectionId.toString(), null, List.of(), JobStatus.SUCCEEDED, 0L, 0L, 0L);

    final long jobOneBytesCommitted = 12345L;
    final long jobOneBytesEmitted = 23456L;
    final long jobOneRecordsCommitted = 19L;
    final long jobOneRecordsEmmitted = 20L;
    final long jobOneCreatedAt = 1000L;
    final long jobOneUpdatedAt = 2000L;
    final long jobTwoCreatedAt = 3000L;
    final long jobTwoUpdatedAt = 4000L;
    final long jobTwoBytesCommitted = 98765L;
    final long jobTwoBytesEmmitted = 87654L;
    final long jobTwoRecordsCommitted = 50L;
    final long jobTwoRecordsEmittted = 60L;
    try (MockedStatic<StatsAggregationHelper> mockStatsAggregationHelper = Mockito.mockStatic(StatsAggregationHelper.class)) {
      mockStatsAggregationHelper.when(() -> StatsAggregationHelper.getJobIdToJobWithAttemptsReadMap(Mockito.any(), Mockito.any()))
          .thenReturn(Map.of(
              jobOneId, new JobWithAttemptsRead().job(
                  new JobRead().createdAt(jobOneCreatedAt).updatedAt(jobOneUpdatedAt).configType(JobConfigType.SYNC).aggregatedStats(
                      new JobAggregatedStats()
                          .bytesCommitted(jobOneBytesCommitted)
                          .bytesEmitted(jobOneBytesEmitted)
                          .recordsCommitted(jobOneRecordsCommitted)
                          .recordsEmitted(jobOneRecordsEmmitted))),
              jobTwoId, new JobWithAttemptsRead().job(
                  new JobRead().createdAt(jobTwoCreatedAt).updatedAt(jobTwoUpdatedAt).configType(JobConfigType.SYNC).aggregatedStats(
                      new JobAggregatedStats()
                          .bytesCommitted(jobTwoBytesCommitted)
                          .bytesEmitted(jobTwoBytesEmmitted)
                          .recordsCommitted(jobTwoRecordsCommitted)
                          .recordsEmitted(jobTwoRecordsEmittted)))));

      List<JobSyncResultRead> expected = List.of(
          new JobSyncResultRead()
              .configType(JobConfigType.SYNC)
              .jobId(jobOneId)
              .bytesCommitted(jobOneBytesCommitted)
              .bytesEmitted(jobOneBytesEmitted)
              .recordsCommitted(jobOneRecordsCommitted)
              .recordsEmitted(jobOneRecordsEmmitted)
              .jobCreatedAt(jobOneCreatedAt)
              .jobUpdatedAt(jobOneUpdatedAt)
              .streamStatuses(List.of(
                  new ConnectionSyncResultRead()
                      .status(io.airbyte.api.model.generated.JobStatus.SUCCEEDED)
                      .streamName("streamOne")
                      .streamNamespace("streamOneNamespace"),
                  new ConnectionSyncResultRead()
                      .status(io.airbyte.api.model.generated.JobStatus.SUCCEEDED)
                      .streamName("streamTwo")
                      .streamNamespace("streamTwoNamespace"))),
          new JobSyncResultRead()
              .configType(JobConfigType.SYNC)
              .jobId(jobTwoId)
              .bytesCommitted(jobTwoBytesCommitted)
              .bytesEmitted(jobTwoBytesEmmitted)
              .recordsCommitted(jobTwoRecordsCommitted)
              .recordsEmitted(jobTwoRecordsEmittted)
              .jobCreatedAt(jobTwoCreatedAt)
              .jobUpdatedAt(jobTwoUpdatedAt)
              .streamStatuses(List.of(
                  new ConnectionSyncResultRead()
                      .status(io.airbyte.api.model.generated.JobStatus.SUCCEEDED)
                      .streamName("streamThree")
                      .streamNamespace("streamThreeNamespace"))));

      when(repo.findLastAttemptsOfLastXJobsForConnection(connectionId, numJobs)).thenReturn(List.of(ssOne, ssTwo, ssThree));
      when(jobPersistence.listJobsLight(Set.of(jobOneId, jobTwoId))).thenReturn(List.of(jobOne, jobTwo));
      var handlerWithRealMapper = new StreamStatusesHandler(repo, new StreamStatusesMapper(), jobHistoryHandler, jobPersistence);
      Assertions.assertEquals(expected, handlerWithRealMapper.getConnectionUptimeHistory(apiReq));
    }
  }

}
