/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.config.Job.SYNC_REPLICATION_TYPES;
import static io.airbyte.featureflag.ContextKt.ANONYMOUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.AttemptInfoRead;
import io.airbyte.api.model.generated.AttemptInfoReadLogs;
import io.airbyte.api.model.generated.AttemptRead;
import io.airbyte.api.model.generated.AttemptStreamStats;
import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.api.model.generated.ConnectionSyncProgressRead;
import io.airbyte.api.model.generated.DestinationIdRequestBody;
import io.airbyte.api.model.generated.DestinationRead;
import io.airbyte.api.model.generated.JobAggregatedStats;
import io.airbyte.api.model.generated.JobConfigType;
import io.airbyte.api.model.generated.JobDebugInfoRead;
import io.airbyte.api.model.generated.JobDebugRead;
import io.airbyte.api.model.generated.JobIdRequestBody;
import io.airbyte.api.model.generated.JobInfoLightRead;
import io.airbyte.api.model.generated.JobInfoRead;
import io.airbyte.api.model.generated.JobListRequestBody;
import io.airbyte.api.model.generated.JobRead;
import io.airbyte.api.model.generated.JobReadList;
import io.airbyte.api.model.generated.JobRefreshConfig;
import io.airbyte.api.model.generated.JobWithAttemptsRead;
import io.airbyte.api.model.generated.LogFormatType;
import io.airbyte.api.model.generated.Pagination;
import io.airbyte.api.model.generated.ResetConfig;
import io.airbyte.api.model.generated.SourceIdRequestBody;
import io.airbyte.api.model.generated.SourceRead;
import io.airbyte.api.model.generated.StreamDescriptor;
import io.airbyte.api.model.generated.StreamStats;
import io.airbyte.api.model.generated.StreamSyncProgressReadItem;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.logging.LogClientManager;
import io.airbyte.commons.logging.LogEvents;
import io.airbyte.commons.logging.LogUtils;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.converters.JobConverter;
import io.airbyte.commons.server.handlers.helpers.CatalogConverter;
import io.airbyte.commons.server.helpers.ConnectionHelpers;
import io.airbyte.commons.server.helpers.DestinationHelpers;
import io.airbyte.commons.server.helpers.SourceHelpers;
import io.airbyte.commons.temporal.TemporalClient;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.config.AirbyteStream;
import io.airbyte.config.Attempt;
import io.airbyte.config.AttemptStatus;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.DestinationSyncMode;
import io.airbyte.config.Job;
import io.airbyte.config.JobCheckConnectionConfig;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.JobStatus;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StreamSyncStats;
import io.airbyte.config.SyncMode;
import io.airbyte.config.SyncStats;
import io.airbyte.config.helpers.FieldGenerator;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.JobService;
import io.airbyte.data.services.impls.jooq.ConnectionServiceJooqImpl;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.HydrateAggregatedStats;
import io.airbyte.featureflag.TestClient;
import io.airbyte.featureflag.Workspace;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.JobPersistence.AttemptStats;
import io.airbyte.persistence.job.JobPersistence.JobAttemptPair;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

@DisplayName("Job History Handler")
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class JobHistoryHandlerTest {

  private final ApiPojoConverters apiPojoConverters = new ApiPojoConverters(new CatalogConverter(new FieldGenerator(), Collections.emptyList()));

  private static final long JOB_ID = 100L;
  private static final String JOB_CONFIG_ID = "ef296385-6796-413f-ac1b-49c4caba3f2b";
  private static final JobStatus JOB_STATUS = JobStatus.SUCCEEDED;
  private static final JobConfig.ConfigType CONFIG_TYPE = ConfigType.SYNC;
  private static final JobConfigType CONFIG_TYPE_FOR_API = JobConfigType.CHECK_CONNECTION_SOURCE;
  private static final JobConfig JOB_CONFIG = new JobConfig()
      .withConfigType(CONFIG_TYPE)
      .withCheckConnection(new JobCheckConnectionConfig())
      .withSync(new JobSyncConfig().withConfiguredAirbyteCatalog(
          new ConfiguredAirbyteCatalog().withStreams(List.of(
              new ConfiguredAirbyteStream(new AirbyteStream("stream1", Jsons.emptyObject(), List.of(SyncMode.FULL_REFRESH)).withNamespace("ns1"),
                  SyncMode.FULL_REFRESH,
                  DestinationSyncMode.APPEND),
              new ConfiguredAirbyteStream(new AirbyteStream("stream2", Jsons.emptyObject(), List.of(SyncMode.INCREMENTAL)), SyncMode.INCREMENTAL,
                  DestinationSyncMode.APPEND)))));
  private static final Path LOG_PATH = Path.of("log_path");
  private static final AttemptInfoReadLogs EMPTY_LOG_READ = new AttemptInfoReadLogs().logLines(new ArrayList<>());
  private static final long CREATED_AT = System.currentTimeMillis() / 1000;

  private static final AttemptStats FIRST_ATTEMPT_STATS = new AttemptStats(new SyncStats()
      .withRecordsEmitted(55L)
      .withBytesEmitted(22L)
      .withRecordsCommitted(55L)
      .withBytesCommitted(22L),
      List.of(
          new StreamSyncStats().withStreamNamespace("ns1").withStreamName("stream1")
              .withStats(new SyncStats()
                  .withRecordsEmitted(5L)
                  .withBytesEmitted(2L)
                  .withRecordsCommitted(5L)
                  .withBytesCommitted(2L)),
          new StreamSyncStats().withStreamName("stream2")
              .withStats(new SyncStats()
                  .withRecordsEmitted(50L)
                  .withBytesEmitted(20L)
                  .withRecordsCommitted(50L)
                  .withBytesCommitted(20L))));

  private static final AttemptStats SECOND_ATTEMPT_STATS = new AttemptStats(new SyncStats()
      .withRecordsEmitted(5500L)
      .withBytesEmitted(2200L)
      .withRecordsCommitted(5500L)
      .withBytesCommitted(2200L),
      List.of(
          new StreamSyncStats().withStreamNamespace("ns1").withStreamName("stream1")
              .withStats(new SyncStats()
                  .withRecordsEmitted(500L)
                  .withBytesEmitted(200L)
                  .withRecordsCommitted(500L)
                  .withBytesCommitted(200L)),
          new StreamSyncStats().withStreamName("stream2")
              .withStats(new SyncStats()
                  .withRecordsEmitted(5000L)
                  .withBytesEmitted(2000L)
                  .withRecordsCommitted(5000L)
                  .withBytesCommitted(2000L))));

  private static final io.airbyte.api.model.generated.AttemptStats FIRST_ATTEMPT_STATS_API = new io.airbyte.api.model.generated.AttemptStats()
      .recordsEmitted(55L)
      .bytesEmitted(22L)
      .recordsCommitted(55L);

  private static final List<AttemptStreamStats> FIRST_ATTEMPT_STREAM_STATS = List.of(
      new AttemptStreamStats()
          .streamNamespace("ns1")
          .streamName("stream1")
          .stats(new io.airbyte.api.model.generated.AttemptStats()
              .recordsEmitted(5L)
              .bytesEmitted(2L)
              .recordsCommitted(5L)),
      new AttemptStreamStats()
          .streamName("stream2")
          .stats(new io.airbyte.api.model.generated.AttemptStats()
              .recordsEmitted(50L)
              .bytesEmitted(20L)
              .recordsCommitted(50L)));

  private static final io.airbyte.api.model.generated.AttemptStats SECOND_ATTEMPT_STATS_API = new io.airbyte.api.model.generated.AttemptStats()
      .recordsEmitted(5500L)
      .bytesEmitted(2200L)
      .recordsCommitted(5500L);

  private static final List<AttemptStreamStats> SECOND_ATTEMPT_STREAM_STATS = List.of(
      new AttemptStreamStats()
          .streamNamespace("ns1")
          .streamName("stream1")
          .stats(new io.airbyte.api.model.generated.AttemptStats()
              .recordsEmitted(500L)
              .bytesEmitted(200L)
              .recordsCommitted(500L)),
      new AttemptStreamStats()
          .streamName("stream2")
          .stats(new io.airbyte.api.model.generated.AttemptStats()
              .recordsEmitted(5000L)
              .bytesEmitted(2000L)
              .recordsCommitted(5000L)));

  private ConnectionService connectionService;
  private SourceHandler sourceHandler;
  private DestinationHandler destinationHandler;
  private Attempt testJobAttempt;
  private JobConverter jobConverter;
  private JobPersistence jobPersistence;
  private LogClientManager logClientManager;
  private LogUtils logUtils;
  private FeatureFlagClient featureFlagClient;
  private JobHistoryHandler jobHistoryHandler;
  private TemporalClient temporalClient;
  private JobService jobService;

  private static JobRead toJobInfo(final Job job) {
    return new JobRead().id(job.getId())
        .configId(job.getScope())
        .enabledStreams(job.getConfig().getSync().getConfiguredAirbyteCatalog().getStreams()
            .stream()
            .map(s -> new StreamDescriptor().name(s.getStream().getName()).namespace(s.getStream().getNamespace()))
            .collect(Collectors.toList()))
        .status(Enums.convertTo(job.getStatus(), io.airbyte.api.model.generated.JobStatus.class))
        .configType(Enums.convertTo(job.getConfigType(), io.airbyte.api.model.generated.JobConfigType.class))
        .createdAt(job.getCreatedAtInSecond())
        .updatedAt(job.getUpdatedAtInSecond());
  }

  private static JobDebugRead toDebugJobInfo(final Job job) {
    return new JobDebugRead().id(job.getId())
        .configId(job.getScope())
        .status(Enums.convertTo(job.getStatus(), io.airbyte.api.model.generated.JobStatus.class))
        .configType(Enums.convertTo(job.getConfigType(), io.airbyte.api.model.generated.JobConfigType.class))
        .sourceDefinition(null)
        .destinationDefinition(null);

  }

  private static List<AttemptInfoRead> toAttemptInfoList(final List<Attempt> attempts) {
    final List<AttemptRead> attemptReads = attempts.stream().map(JobHistoryHandlerTest::toAttemptRead).toList();

    final Function<AttemptRead, AttemptInfoRead> toAttemptInfoRead =
        (AttemptRead a) -> new AttemptInfoRead().attempt(a).logType(LogFormatType.FORMATTED).logs(EMPTY_LOG_READ);
    return attemptReads.stream().map(toAttemptInfoRead).collect(Collectors.toList());
  }

  private static AttemptRead toAttemptRead(final Attempt a) {
    return new AttemptRead()
        .id((long) a.getAttemptNumber())
        .status(Enums.convertTo(a.getStatus(), io.airbyte.api.model.generated.AttemptStatus.class))
        .streamStats(null)
        .createdAt(a.getCreatedAtInSecond())
        .updatedAt(a.getUpdatedAtInSecond())
        .endedAt(a.getEndedAtInSecond().orElse(null));
  }

  private static Attempt createAttempt(final int attemptNumber, final long jobId, final long timestamps, final AttemptStatus status) {
    return new Attempt(attemptNumber, jobId, LOG_PATH, null, null, status, null, null, timestamps, timestamps, timestamps);
  }

  @BeforeEach
  void setUp() {
    testJobAttempt = createAttempt(0, JOB_ID, CREATED_AT, AttemptStatus.SUCCEEDED);

    connectionService = mock(ConnectionServiceJooqImpl.class);
    sourceHandler = mock(SourceHandler.class);
    destinationHandler = mock(DestinationHandler.class);
    jobPersistence = mock(JobPersistence.class);
    logClientManager = mock(LogClientManager.class);
    logUtils = mock(LogUtils.class);
    featureFlagClient = mock(TestClient.class);
    temporalClient = mock(TemporalClient.class);
    jobConverter = new JobConverter(logClientManager, logUtils);
    final SourceDefinitionsHandler sourceDefinitionsHandler = mock(SourceDefinitionsHandler.class);
    final DestinationDefinitionsHandler destinationDefinitionsHandler = mock(DestinationDefinitionsHandler.class);
    final AirbyteVersion airbyteVersion = mock(AirbyteVersion.class);
    jobService = mock(JobService.class);
    jobHistoryHandler = new JobHistoryHandler(
        jobPersistence,
        connectionService,
        sourceHandler,
        sourceDefinitionsHandler,
        destinationHandler,
        destinationDefinitionsHandler,
        airbyteVersion,
        temporalClient,
        featureFlagClient,
        jobConverter,
        jobService,
        apiPojoConverters);
  }

  @Nested
  @DisplayName("When listing jobs")
  class ListJobs {

    @Test
    @DisplayName("Should return jobs with/without attempts in descending order")
    void testListJobs() throws IOException {
      when(featureFlagClient.boolVariation(HydrateAggregatedStats.INSTANCE, new Workspace(ANONYMOUS))).thenReturn(true);
      Attempt successfulJobAttempt2 = createAttempt(1, JOB_ID, CREATED_AT, AttemptStatus.SUCCEEDED);
      final var successfulJob = new Job(JOB_ID, JOB_CONFIG.getConfigType(), JOB_CONFIG_ID, JOB_CONFIG,
          List.of(testJobAttempt, successfulJobAttempt2), JOB_STATUS, null, CREATED_AT,
          CREATED_AT);
      final int pagesize = 25;
      final int rowOffset = 0;

      final var jobId2 = JOB_ID + 100;
      final var createdAt2 = CREATED_AT + 1000;
      final var latestJobNoAttempt =
          new Job(jobId2, JOB_CONFIG.getConfigType(), JOB_CONFIG_ID, JOB_CONFIG, Collections.emptyList(), JobStatus.PENDING,
              null, createdAt2, createdAt2);

      when(jobService.listJobs(eq(Set.of(Enums.convertTo(CONFIG_TYPE_FOR_API, ConfigType.class))),
          eq(JOB_CONFIG_ID),
          eq(pagesize),
          eq(rowOffset),
          any(),
          any(),
          any(),
          any(),
          any(),
          any(),
          any()))
              .thenReturn(List.of(latestJobNoAttempt, successfulJob));
      when(jobPersistence.getJobCount(eq(Set.of(Enums.convertTo(CONFIG_TYPE_FOR_API, ConfigType.class))), eq(JOB_CONFIG_ID), any(), any(), any(),
          any(), any()))
              .thenReturn(2L);
      when(jobPersistence.getAttemptStats(List.of(200L, 100L))).thenReturn(Map.of(
          new JobAttemptPair(100, 0), FIRST_ATTEMPT_STATS,
          new JobAttemptPair(100, 1), SECOND_ATTEMPT_STATS,
          new JobAttemptPair(jobId2, 0), FIRST_ATTEMPT_STATS));

      final var requestBody = new JobListRequestBody()
          .configTypes(Collections.singletonList(CONFIG_TYPE_FOR_API))
          .configId(JOB_CONFIG_ID)
          .pagination(new Pagination().pageSize(pagesize).rowOffset(rowOffset));
      final var jobReadList = jobHistoryHandler.listJobsFor(requestBody);

      final var expectedAttemptRead1 = toAttemptRead(testJobAttempt).totalStats(FIRST_ATTEMPT_STATS_API).streamStats(FIRST_ATTEMPT_STREAM_STATS);
      final var expectedAttemptRead2 =
          toAttemptRead(successfulJobAttempt2).totalStats(SECOND_ATTEMPT_STATS_API).streamStats(SECOND_ATTEMPT_STREAM_STATS);
      final var successfulJobWithAttemptRead = new JobWithAttemptsRead().job(toJobInfo(successfulJob)
          .aggregatedStats(new JobAggregatedStats()
              .recordsEmitted(5550L)
              .bytesEmitted(2220L)
              .recordsCommitted(5550L)
              .bytesCommitted(2220L))
          .streamAggregatedStats(List.of(
              new StreamStats()
                  .streamName("stream2")
                  .recordsEmitted(5050L)
                  .bytesEmitted(2020L)
                  .recordsCommitted(5050L)
                  .bytesCommitted(2020L),
              new StreamStats()
                  .streamName("stream1")
                  .streamNamespace("ns1")
                  .recordsEmitted(500L)
                  .bytesEmitted(200L)
                  .recordsCommitted(500L)
                  .bytesCommitted(200L))))
          .attempts(List.of(expectedAttemptRead1, expectedAttemptRead2));
      final var latestJobWithAttemptRead = new JobWithAttemptsRead().job(toJobInfo(latestJobNoAttempt)
          .aggregatedStats(new JobAggregatedStats()
              .recordsEmitted(0L)
              .bytesEmitted(0L)
              .recordsCommitted(0L)
              .bytesCommitted(0L))
          .streamAggregatedStats(Collections.emptyList()))
          .attempts(Collections.emptyList());
      final JobReadList expectedJobReadList =
          new JobReadList().jobs(List.of(latestJobWithAttemptRead, successfulJobWithAttemptRead)).totalJobCount(2L);

      assertEquals(expectedJobReadList, jobReadList);
    }

    @Test
    @DisplayName("Should return jobs in descending order regardless of type")
    void testListJobsFor() throws IOException {
      when(featureFlagClient.boolVariation(HydrateAggregatedStats.INSTANCE, new Workspace(ANONYMOUS))).thenReturn(true);
      final var firstJob =
          new Job(JOB_ID, JOB_CONFIG.getConfigType(), JOB_CONFIG_ID, JOB_CONFIG, List.of(testJobAttempt), JOB_STATUS, null, CREATED_AT,
              CREATED_AT);
      final int pagesize = 25;
      final int rowOffset = 0;

      final var secondJobId = JOB_ID + 100;
      final var createdAt2 = CREATED_AT + 1000;
      final var secondJobAttempt = createAttempt(0, secondJobId, createdAt2, AttemptStatus.SUCCEEDED);
      final var secondJob = new Job(secondJobId, ConfigType.SYNC, JOB_CONFIG_ID, JOB_CONFIG, List.of(secondJobAttempt),
          JobStatus.SUCCEEDED, null, createdAt2, createdAt2);

      final Set<ConfigType> configTypes = Set.of(
          Enums.convertTo(CONFIG_TYPE_FOR_API, ConfigType.class),
          Enums.convertTo(JobConfigType.SYNC, ConfigType.class),
          Enums.convertTo(JobConfigType.DISCOVER_SCHEMA, ConfigType.class));

      final var latestJobId = secondJobId + 100;
      final var createdAt3 = createdAt2 + 1000;
      final var latestJob =
          new Job(latestJobId, ConfigType.SYNC, JOB_CONFIG_ID, JOB_CONFIG, Collections.emptyList(), JobStatus.PENDING, null, createdAt3, createdAt3);

      when(jobService.listJobs(eq(configTypes), eq(JOB_CONFIG_ID), eq(pagesize), eq(rowOffset), any(), any(), any(), any(), any(), any(), any()))
          .thenReturn(List.of(latestJob, secondJob, firstJob));
      when(jobPersistence.getJobCount(eq(configTypes), eq(JOB_CONFIG_ID), any(), any(), any(), any(), any())).thenReturn(3L);
      when(jobPersistence.getAttemptStats(List.of(300L, 200L, 100L))).thenReturn(Map.of(
          new JobAttemptPair(100, 0), FIRST_ATTEMPT_STATS,
          new JobAttemptPair(secondJobId, 0), FIRST_ATTEMPT_STATS,
          new JobAttemptPair(latestJobId, 0), FIRST_ATTEMPT_STATS));

      final JobListRequestBody requestBody = new JobListRequestBody()
          .configTypes(List.of(CONFIG_TYPE_FOR_API, JobConfigType.SYNC, JobConfigType.DISCOVER_SCHEMA))
          .configId(JOB_CONFIG_ID)
          .pagination(new Pagination().pageSize(pagesize).rowOffset(rowOffset));
      final JobReadList jobReadList = jobHistoryHandler.listJobsFor(requestBody);

      final var firstJobWithAttemptRead =
          new JobWithAttemptsRead().job(toJobInfo(firstJob)
              .aggregatedStats(new JobAggregatedStats()
                  .recordsEmitted(55L)
                  .bytesEmitted(22L)
                  .recordsCommitted(55L)
                  .bytesCommitted(22L))
              .streamAggregatedStats(List.of(
                  new StreamStats()
                      .streamName("stream2")
                      .recordsEmitted(50L)
                      .bytesEmitted(20L)
                      .recordsCommitted(50L)
                      .bytesCommitted(20L),
                  new StreamStats()
                      .streamName("stream1")
                      .streamNamespace("ns1")
                      .recordsEmitted(5L)
                      .bytesEmitted(2L)
                      .recordsCommitted(5L)
                      .bytesCommitted(2L))))
              .attempts(List.of(toAttemptRead(testJobAttempt).totalStats(FIRST_ATTEMPT_STATS_API).streamStats(FIRST_ATTEMPT_STREAM_STATS)));
      final var secondJobWithAttemptRead =
          new JobWithAttemptsRead().job(toJobInfo(secondJob)
              .aggregatedStats(new JobAggregatedStats()
                  .recordsEmitted(55L)
                  .bytesEmitted(22L)
                  .recordsCommitted(55L)
                  .bytesCommitted(22L))
              .streamAggregatedStats(List.of(
                  new StreamStats()
                      .streamName("stream2")
                      .recordsEmitted(50L)
                      .bytesEmitted(20L)
                      .recordsCommitted(50L)
                      .bytesCommitted(20L),
                  new StreamStats()
                      .streamName("stream1")
                      .streamNamespace("ns1")
                      .recordsEmitted(5L)
                      .bytesEmitted(2L)
                      .recordsCommitted(5L)
                      .bytesCommitted(2L))))
              .attempts(
                  List.of(toAttemptRead(secondJobAttempt).totalStats(FIRST_ATTEMPT_STATS_API).streamStats(FIRST_ATTEMPT_STREAM_STATS)));
      final var latestJobWithAttemptRead = new JobWithAttemptsRead().job(toJobInfo(latestJob)
          .aggregatedStats(new JobAggregatedStats()
              .recordsEmitted(0L)
              .bytesEmitted(0L)
              .recordsCommitted(0L)
              .bytesCommitted(0L))
          .streamAggregatedStats(Collections.emptyList())).attempts(Collections.emptyList());
      final JobReadList expectedJobReadList =
          new JobReadList().jobs(List.of(latestJobWithAttemptRead, secondJobWithAttemptRead, firstJobWithAttemptRead)).totalJobCount(3L);

      assertEquals(expectedJobReadList, jobReadList);
    }

    @Test
    @DisplayName("Should return jobs including specified job id")
    void testListJobsIncludingJobId() throws IOException {
      when(featureFlagClient.boolVariation(HydrateAggregatedStats.INSTANCE, new Workspace(ANONYMOUS))).thenReturn(true);
      final var successfulJob =
          new Job(JOB_ID, JOB_CONFIG.getConfigType(), JOB_CONFIG_ID, JOB_CONFIG, List.of(testJobAttempt), JOB_STATUS, null, CREATED_AT,
              CREATED_AT);
      final int pagesize = 25;
      final int rowOffset = 0;

      final var jobId2 = JOB_ID + 100;
      final var createdAt2 = CREATED_AT + 1000;
      final var latestJobNoAttempt =
          new Job(jobId2, JOB_CONFIG.getConfigType(), JOB_CONFIG_ID, JOB_CONFIG, Collections.emptyList(), JobStatus.PENDING,
              null, createdAt2, createdAt2);

      when(jobPersistence.listJobsIncludingId(Set.of(Enums.convertTo(CONFIG_TYPE_FOR_API, ConfigType.class)), JOB_CONFIG_ID, jobId2, pagesize))
          .thenReturn(List.of(latestJobNoAttempt, successfulJob));
      when(jobPersistence.getJobCount(eq(Set.of(Enums.convertTo(CONFIG_TYPE_FOR_API, ConfigType.class))), eq(JOB_CONFIG_ID), any(), any(), any(),
          any(), any()))
              .thenReturn(2L);
      when(jobPersistence.getAttemptStats(List.of(200L, 100L))).thenReturn(Map.of(
          new JobAttemptPair(100, 0), FIRST_ATTEMPT_STATS,
          new JobAttemptPair(jobId2, 0), FIRST_ATTEMPT_STATS));

      final var requestBody = new JobListRequestBody()
          .configTypes(Collections.singletonList(CONFIG_TYPE_FOR_API))
          .configId(JOB_CONFIG_ID)
          .includingJobId(jobId2)
          .pagination(new Pagination().pageSize(pagesize).rowOffset(rowOffset));
      final var jobReadList = jobHistoryHandler.listJobsFor(requestBody);

      final var successfulJobWithAttemptRead = new JobWithAttemptsRead().job(toJobInfo(successfulJob)
          .aggregatedStats(new JobAggregatedStats()
              .recordsEmitted(55L)
              .bytesEmitted(22L)
              .recordsCommitted(55L)
              .bytesCommitted(22L))
          .streamAggregatedStats(List.of(
              new StreamStats()
                  .streamName("stream2")
                  .recordsEmitted(50L)
                  .bytesEmitted(20L)
                  .recordsCommitted(50L)
                  .bytesCommitted(20L),
              new StreamStats()
                  .streamName("stream1")
                  .streamNamespace("ns1")
                  .recordsEmitted(5L)
                  .bytesEmitted(2L)
                  .recordsCommitted(5L)
                  .bytesCommitted(2L))))
          .attempts(List.of(toAttemptRead(
              testJobAttempt).totalStats(FIRST_ATTEMPT_STATS_API).streamStats(FIRST_ATTEMPT_STREAM_STATS)));
      final var latestJobWithAttemptRead = new JobWithAttemptsRead().job(toJobInfo(latestJobNoAttempt)
          .aggregatedStats(new JobAggregatedStats()
              .recordsEmitted(0L)
              .bytesEmitted(0L)
              .recordsCommitted(0L)
              .bytesCommitted(0L))
          .streamAggregatedStats(Collections.emptyList()))
          .attempts(Collections.emptyList());
      final JobReadList expectedJobReadList =
          new JobReadList().jobs(List.of(latestJobWithAttemptRead, successfulJobWithAttemptRead)).totalJobCount(2L);

      assertEquals(expectedJobReadList, jobReadList);
    }

  }

  @Test
  @DisplayName("Should return the right job info")
  void testGetJobInfo() throws IOException {
    Job job = new Job(JOB_ID, JOB_CONFIG.getConfigType(), JOB_CONFIG_ID, JOB_CONFIG, List.of(testJobAttempt), JOB_STATUS, null, CREATED_AT,
        CREATED_AT);
    when(jobPersistence.getJob(JOB_ID)).thenReturn(job);
    when(logClientManager.getLogs(any())).thenReturn(new LogEvents(List.of(), "1"));

    final JobIdRequestBody requestBody = new JobIdRequestBody().id(JOB_ID);
    final JobInfoRead jobInfoActual = jobHistoryHandler.getJobInfo(requestBody);

    final JobInfoRead exp = new JobInfoRead().job(toJobInfo(job)).attempts(toAttemptInfoList(List.of(testJobAttempt)));

    assertEquals(exp, jobInfoActual);
  }

  @Test
  @DisplayName("Should return the right job info without attempt information")
  void testGetJobInfoLight() throws IOException {
    Job job = new Job(JOB_ID, JOB_CONFIG.getConfigType(), JOB_CONFIG_ID, JOB_CONFIG, List.of(testJobAttempt), JOB_STATUS, null, CREATED_AT,
        CREATED_AT);
    when(jobPersistence.getJob(JOB_ID)).thenReturn(job);

    final JobIdRequestBody requestBody = new JobIdRequestBody().id(JOB_ID);
    final JobInfoLightRead jobInfoLightActual = jobHistoryHandler.getJobInfoLight(requestBody);

    final JobInfoLightRead exp = new JobInfoLightRead().job(toJobInfo(job));

    assertEquals(exp, jobInfoLightActual);
  }

  @Test
  @DisplayName("Should return the right info to debug this job")
  void testGetDebugJobInfo()
      throws IOException, JsonValidationException, ConfigNotFoundException, io.airbyte.data.exceptions.ConfigNotFoundException {
    Job job = new Job(JOB_ID, JOB_CONFIG.getConfigType(), JOB_CONFIG_ID, JOB_CONFIG, List.of(testJobAttempt), JOB_STATUS, null, CREATED_AT,
        CREATED_AT);
    final StandardSourceDefinition standardSourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withName("marketo");
    final SourceConnection source = SourceHelpers.generateSource(UUID.randomUUID());
    final SourceRead sourceRead = SourceHelpers.getSourceRead(source, standardSourceDefinition);

    final StandardDestinationDefinition standardDestinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID.randomUUID())
        .withName("db2");
    final DestinationConnection destination = DestinationHelpers.generateDestination(UUID.randomUUID());
    final DestinationRead destinationRead = DestinationHelpers.getDestinationRead(destination, standardDestinationDefinition);

    final StandardSync standardSync = ConnectionHelpers.generateSyncWithSourceId(source.getSourceId());
    final ConnectionRead connectionRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync);
    when(connectionService.getStandardSync(UUID.fromString(job.getScope()))).thenReturn(standardSync);

    final SourceIdRequestBody sourceIdRequestBody = new SourceIdRequestBody();
    sourceIdRequestBody.setSourceId(connectionRead.getSourceId());
    when(sourceHandler.getSource(sourceIdRequestBody)).thenReturn(sourceRead);

    final DestinationIdRequestBody destinationIdRequestBody = new DestinationIdRequestBody();
    destinationIdRequestBody.setDestinationId(connectionRead.getDestinationId());
    when(destinationHandler.getDestination(destinationIdRequestBody)).thenReturn(destinationRead);
    when(jobPersistence.getJob(JOB_ID)).thenReturn(job);
    when(jobPersistence.getAttemptStats(anyLong(), anyInt())).thenReturn(FIRST_ATTEMPT_STATS);
    when(logClientManager.getLogs(any())).thenReturn(new LogEvents(List.of(), "1"));

    final JobIdRequestBody requestBody = new JobIdRequestBody().id(JOB_ID);
    final JobDebugInfoRead jobDebugInfoActual = jobHistoryHandler.getJobDebugInfo(requestBody);
    final List<AttemptInfoRead> attemptInfoReads = toAttemptInfoList(List.of(testJobAttempt));
    attemptInfoReads.forEach(read -> read.getAttempt().totalStats(FIRST_ATTEMPT_STATS_API).streamStats(FIRST_ATTEMPT_STREAM_STATS));
    final JobDebugInfoRead exp = new JobDebugInfoRead().job(toDebugJobInfo(job)).attempts(attemptInfoReads);

    assertEquals(exp, jobDebugInfoActual);
  }

  @Test
  @DisplayName("Should return the latest running sync job")
  void testGetLatestRunningSyncJob() throws IOException {
    final var connectionId = UUID.randomUUID();

    final var olderRunningJobId = JOB_ID + 100;
    final var olderRunningCreatedAt = CREATED_AT + 1000;
    final var olderRunningJobAttempt = createAttempt(0, olderRunningJobId, olderRunningCreatedAt, AttemptStatus.RUNNING);
    final var olderRunningJob = new Job(olderRunningJobId, ConfigType.SYNC, JOB_CONFIG_ID,
        JOB_CONFIG, List.of(olderRunningJobAttempt),
        JobStatus.RUNNING, null, olderRunningCreatedAt, olderRunningCreatedAt);

    // expect that we return the newer of the two running jobs. this should not happen in the real
    // world but might as
    // well test that we handle it properly.
    final var newerRunningJobId = JOB_ID + 200;
    final var newerRunningCreatedAt = CREATED_AT + 2000;
    final var newerRunningJobAttempt = createAttempt(0, newerRunningJobId, newerRunningCreatedAt, AttemptStatus.RUNNING);
    final var newerRunningJob = new Job(newerRunningJobId, ConfigType.SYNC, JOB_CONFIG_ID,
        JOB_CONFIG, List.of(newerRunningJobAttempt),
        JobStatus.RUNNING, null, newerRunningCreatedAt, newerRunningCreatedAt);

    when(jobPersistence.listJobsForConnectionWithStatuses(
        connectionId,
        SYNC_REPLICATION_TYPES,
        JobStatus.NON_TERMINAL_STATUSES)).thenReturn(List.of(newerRunningJob, olderRunningJob));

    final Optional<JobRead> expectedJob = Optional.of(JobConverter.getJobRead(newerRunningJob));
    final Optional<JobRead> actualJob = jobHistoryHandler.getLatestRunningSyncJob(connectionId);

    assertEquals(expectedJob, actualJob);
  }

  @Test
  @DisplayName("Should return an empty optional if no running sync job")
  void testGetLatestRunningSyncJobWhenNone() throws IOException {
    final var connectionId = UUID.randomUUID();

    when(jobPersistence.listJobsForConnectionWithStatuses(
        connectionId,
        SYNC_REPLICATION_TYPES,
        JobStatus.NON_TERMINAL_STATUSES)).thenReturn(Collections.emptyList());

    final Optional<JobRead> actual = jobHistoryHandler.getLatestRunningSyncJob(connectionId);

    assertTrue(actual.isEmpty());
  }

  @Nested
  @DisplayName("Sync progress")
  class ConnectionSyncProgressTests {

    @Test
    @DisplayName("Should not throw with no running sync")
    void testGetConnectionSyncProgressNoJobs() throws IOException {
      final UUID connectionId = UUID.randomUUID();

      final ConnectionIdRequestBody request = new ConnectionIdRequestBody().connectionId(connectionId);

      final ConnectionSyncProgressRead expectedSyncProgress =
          new ConnectionSyncProgressRead().connectionId(connectionId).streams(Collections.emptyList());

      final ConnectionSyncProgressRead actual = jobHistoryHandler.getConnectionSyncProgress(request);

      assertEquals(expectedSyncProgress, actual);
    }

    @Test
    @DisplayName("Should return data for a running sync")
    void testGetConnectionSyncProgressWithRunningJob() throws IOException {
      final UUID connectionId = UUID.randomUUID();
      final ConnectionIdRequestBody request = new ConnectionIdRequestBody().connectionId(connectionId);

      final Job firstJob = new Job(JOB_ID, JOB_CONFIG.getConfigType(), JOB_CONFIG_ID, JOB_CONFIG, List.of(testJobAttempt), JobStatus.RUNNING,
          CREATED_AT, CREATED_AT, CREATED_AT);

      final JobRead jobRead = toJobInfo(firstJob);
      jobRead.setEnabledStreams(List.of(new StreamDescriptor().name("stream1").namespace("ns1"), new StreamDescriptor().name("stream2"),
          new StreamDescriptor().name("stream3")));

      jobRead.setStreamAggregatedStats(List.of(
          new StreamStats().streamName("stream2").recordsEmitted(50L).bytesEmitted(20L).recordsCommitted(45L).bytesCommitted(15L),
          new StreamStats().streamName("stream1").streamNamespace("ns1").recordsEmitted(5L).bytesEmitted(2L).recordsCommitted(5L)
              .bytesCommitted(2L)));

      final JobAggregatedStats jobAggregatedStats =
          new JobAggregatedStats().bytesCommitted(17L).recordsCommitted(50L).bytesEmitted(22L).recordsEmitted(55L);
      jobRead.setAggregatedStats(jobAggregatedStats);

      final JobWithAttemptsRead firstJobWithAttemptRead = new JobWithAttemptsRead()
          .job(jobRead)
          .attempts(List.of(toAttemptRead(testJobAttempt)));

      when(jobPersistence.getRunningJobForConnection(connectionId)).thenReturn(List.of(firstJob));
      try (final MockedStatic<JobConverter> mockedConverter = Mockito.mockStatic(JobConverter.class)) {
        mockedConverter.when(() -> JobConverter.getJobWithAttemptsRead(firstJob)).thenReturn(firstJobWithAttemptRead);

        final ConnectionSyncProgressRead expected = new ConnectionSyncProgressRead()
            .jobId(JOB_ID)
            .connectionId(connectionId)
            .bytesCommitted(jobAggregatedStats.getBytesCommitted())
            .recordsCommitted(jobAggregatedStats.getRecordsCommitted())
            .bytesEmitted(jobAggregatedStats.getBytesEmitted())
            .recordsEmitted(jobAggregatedStats.getRecordsEmitted())
            .configType(JobConfigType.SYNC)
            .syncStartedAt(CREATED_AT)
            .streams(List.of(
                new StreamSyncProgressReadItem()
                    .streamName("stream1")
                    .streamNamespace("ns1")
                    .recordsEmitted(5L)
                    .bytesEmitted(2L)
                    .recordsCommitted(5L)
                    .bytesCommitted(2L)
                    .configType(JobConfigType.SYNC),
                new StreamSyncProgressReadItem()
                    .streamName("stream2")
                    .recordsEmitted(50L)
                    .bytesEmitted(20L)
                    .recordsCommitted(45L)
                    .bytesCommitted(15L)
                    .configType(JobConfigType.SYNC),
                new StreamSyncProgressReadItem()
                    .streamName("stream3")
                    .configType(JobConfigType.SYNC)));

        final ConnectionSyncProgressRead actual = jobHistoryHandler.getConnectionSyncProgress(request);

        assertEquals(expected, actual);
      }
    }

    @Test
    @DisplayName("Should return data for a running refresh")
    void testGetConnectionSyncProgressWithRefresh() throws IOException {
      final UUID connectionId = UUID.randomUUID();
      final ConnectionIdRequestBody request = new ConnectionIdRequestBody().connectionId(connectionId);

      final Job firstJob = new Job(JOB_ID, ConfigType.REFRESH, JOB_CONFIG_ID, JOB_CONFIG, List.of(testJobAttempt), JobStatus.RUNNING,
          CREATED_AT, CREATED_AT, CREATED_AT);

      final JobRead jobRead = toJobInfo(firstJob);
      jobRead.setEnabledStreams(List.of(new StreamDescriptor().name("stream2"),
          new StreamDescriptor().name("stream3")));
      jobRead.setRefreshConfig(new JobRefreshConfig().streamsToRefresh(List.of(
          new StreamDescriptor().name("stream2"))));

      jobRead.setStreamAggregatedStats(List.of(
          new StreamStats().streamName("stream2").recordsEmitted(50L).bytesEmitted(20L).recordsCommitted(45L).bytesCommitted(15L),
          new StreamStats().streamName("stream1").streamNamespace("ns1").recordsEmitted(5L).bytesEmitted(2L).recordsCommitted(5L)
              .bytesCommitted(2L)));

      final JobAggregatedStats jobAggregatedStats =
          new JobAggregatedStats().bytesCommitted(17L).recordsCommitted(50L).bytesEmitted(22L).recordsEmitted(55L);
      jobRead.setAggregatedStats(jobAggregatedStats);

      final JobWithAttemptsRead firstJobWithAttemptRead = new JobWithAttemptsRead()
          .job(jobRead)
          .attempts(List.of(toAttemptRead(testJobAttempt)));

      when(jobPersistence.getRunningJobForConnection(connectionId)).thenReturn(List.of(firstJob));
      try (final MockedStatic<JobConverter> mockedConverter = Mockito.mockStatic(JobConverter.class)) {
        mockedConverter.when(() -> JobConverter.getJobWithAttemptsRead(firstJob)).thenReturn(firstJobWithAttemptRead);

        final ConnectionSyncProgressRead expected = new ConnectionSyncProgressRead()
            .jobId(JOB_ID)
            .connectionId(connectionId)
            .bytesCommitted(jobAggregatedStats.getBytesCommitted())
            .recordsCommitted(jobAggregatedStats.getRecordsCommitted())
            .bytesEmitted(jobAggregatedStats.getBytesEmitted())
            .recordsEmitted(jobAggregatedStats.getRecordsEmitted())
            .configType(JobConfigType.REFRESH)
            .syncStartedAt(CREATED_AT)
            .streams(List.of(
                new StreamSyncProgressReadItem()
                    .streamName("stream3")
                    .configType(JobConfigType.SYNC),
                new StreamSyncProgressReadItem()
                    .streamName("stream2")
                    .recordsEmitted(50L)
                    .bytesEmitted(20L)
                    .recordsCommitted(45L)
                    .bytesCommitted(15L)
                    .configType(JobConfigType.REFRESH)));

        final ConnectionSyncProgressRead actual = jobHistoryHandler.getConnectionSyncProgress(request);

        assertEquals(expected.getStreams(), actual.getStreams());
      }
    }

    @Test
    @DisplayName("Should return data for a running clear")
    void testGetConnectionSyncProgressWithClear() throws IOException {
      final UUID connectionId = UUID.randomUUID();
      final ConnectionIdRequestBody request = new ConnectionIdRequestBody().connectionId(connectionId);

      final Job firstJob =
          new Job(JOB_ID, ConfigType.RESET_CONNECTION, JOB_CONFIG_ID, JOB_CONFIG, List.of(testJobAttempt), JobStatus.RUNNING,
              CREATED_AT, CREATED_AT, CREATED_AT);

      final JobRead jobRead = toJobInfo(firstJob);
      jobRead.setResetConfig(new ResetConfig().streamsToReset(List.of(
          new StreamDescriptor().name("stream1").namespace("ns1"))));
      jobRead.setEnabledStreams(List.of(new StreamDescriptor().name("stream1").namespace("ns1"), new StreamDescriptor().name("stream2"),
          new StreamDescriptor().name("stream3")));

      final JobWithAttemptsRead firstJobWithAttemptRead = new JobWithAttemptsRead()
          .job(jobRead)
          .attempts(List.of(toAttemptRead(testJobAttempt)));

      when(jobPersistence.getRunningJobForConnection(connectionId)).thenReturn(List.of(firstJob));
      try (final MockedStatic<JobConverter> mockedConverter = Mockito.mockStatic(JobConverter.class)) {
        mockedConverter.when(() -> JobConverter.getJobWithAttemptsRead(firstJob)).thenReturn(firstJobWithAttemptRead);

        final ConnectionSyncProgressRead expected = new ConnectionSyncProgressRead()
            .connectionId(connectionId)
            .jobId(JOB_ID)
            .configType(JobConfigType.RESET_CONNECTION)
            .syncStartedAt(CREATED_AT)
            .streams(List.of(
                new StreamSyncProgressReadItem()
                    .streamName("stream1")
                    .streamNamespace("ns1")
                    .configType(JobConfigType.RESET_CONNECTION)));

        final ConnectionSyncProgressRead actual = jobHistoryHandler.getConnectionSyncProgress(request);

        assertEquals(expected, actual);
      }
    }

  }

  @Test
  @DisplayName("Should return the latest sync job")
  void testGetLatestSyncJob() throws IOException {
    final var connectionId = UUID.randomUUID();

    // expect the newest job overall to be returned, even if it is failed
    final var newerFailedJobId = JOB_ID + 200;
    final var newerFailedCreatedAt = CREATED_AT + 2000;
    final var newerFailedJobAttempt = createAttempt(0, newerFailedJobId, newerFailedCreatedAt, AttemptStatus.FAILED);
    final var newerFailedJob = new Job(newerFailedJobId, ConfigType.SYNC, JOB_CONFIG_ID,
        JOB_CONFIG, List.of(newerFailedJobAttempt),
        JobStatus.RUNNING, null, newerFailedCreatedAt, newerFailedCreatedAt);

    when(jobPersistence.getLastSyncJob(connectionId)).thenReturn(Optional.of(newerFailedJob));

    final Optional<JobRead> expectedJob = Optional.of(JobConverter.getJobRead(newerFailedJob));
    final Optional<JobRead> actualJob = jobHistoryHandler.getLatestSyncJob(connectionId);

    assertEquals(expectedJob, actualJob);
  }

  @Test
  @DisplayName("Should have compatible config enums")
  void testEnumConversion() {
    assertTrue(Enums.isCompatible(JobConfig.ConfigType.class, JobConfigType.class));
  }

  @Test
  @DisplayName("Should test to ensure that JobInfoReadWithoutLogs includes the bytes and records committed")
  void testGetJobInfoWithoutLogs() throws IOException {

    when(jobPersistence.getJob(JOB_ID))
        .thenReturn(new Job(JOB_ID, JOB_CONFIG.getConfigType(), JOB_CONFIG_ID, JOB_CONFIG, List.of(testJobAttempt),
            JOB_STATUS, null, CREATED_AT, CREATED_AT));
    when(jobPersistence.getAttemptStats(List.of(JOB_ID)))
        .thenReturn(Map.of(new JobAttemptPair(JOB_ID, testJobAttempt.getAttemptNumber()), FIRST_ATTEMPT_STATS));

    final JobInfoRead resultingJobInfo = jobHistoryHandler.getJobInfoWithoutLogs(new JobIdRequestBody().id(JOB_ID));
    assertEquals(resultingJobInfo.getJob().getAggregatedStats().getBytesCommitted(), FIRST_ATTEMPT_STATS.combinedStats().getBytesCommitted());
    assertEquals(resultingJobInfo.getJob().getAggregatedStats().getRecordsCommitted(), FIRST_ATTEMPT_STATS.combinedStats().getRecordsCommitted());

  }

}
