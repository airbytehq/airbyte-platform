/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job;

import static io.airbyte.db.instance.jobs.jooq.generated.Tables.AIRBYTE_METADATA;
import static io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS;
import static io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS;
import static io.airbyte.db.instance.jobs.jooq.generated.Tables.STREAM_ATTEMPT_METADATA;
import static io.airbyte.db.instance.jobs.jooq.generated.Tables.STREAM_STATS;
import static io.airbyte.db.instance.jobs.jooq.generated.Tables.SYNC_STATS;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.Version;
import io.airbyte.config.Attempt;
import io.airbyte.config.AttemptFailureSummary;
import io.airbyte.config.AttemptStatus;
import io.airbyte.config.AttemptSyncConfig;
import io.airbyte.config.AttemptWithJobInfo;
import io.airbyte.config.FailureReason;
import io.airbyte.config.FailureReason.FailureOrigin;
import io.airbyte.config.Job;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.JobGetSpecConfig;
import io.airbyte.config.JobOutput;
import io.airbyte.config.JobStatus;
import io.airbyte.config.JobStatusSummary;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.StandardSyncSummary;
import io.airbyte.config.State;
import io.airbyte.config.StreamSyncStats;
import io.airbyte.config.SyncStats;
import io.airbyte.db.Database;
import io.airbyte.db.factory.DSLContextFactory;
import io.airbyte.db.factory.DataSourceFactory;
import io.airbyte.db.instance.DatabaseConstants;
import io.airbyte.db.instance.test.TestDatabaseProviders;
import io.airbyte.persistence.job.JobPersistence.AttemptStats;
import io.airbyte.persistence.job.JobPersistence.JobAttemptPair;
import io.airbyte.test.utils.Databases;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.PostgreSQLContainer;

@SuppressWarnings({"PMD.JUnitTestsShouldIncludeAssert", "PMD.AvoidDuplicateLiterals"})
@DisplayName("DefaultJobPersistence")
class DefaultJobPersistenceTest {

  private static final Instant NOW = Instant.now();
  private static final Path LOG_PATH = Path.of("/tmp/logs/all/the/way/down");
  private static final UUID CONNECTION_ID = UUID.randomUUID();
  private static final String SCOPE = CONNECTION_ID.toString();
  private static final UUID CONNECTION_ID2 = UUID.randomUUID();
  private static final JobConfig SPEC_JOB_CONFIG = new JobConfig()
      .withConfigType(ConfigType.GET_SPEC)
      .withGetSpec(new JobGetSpecConfig());

  private static final JobConfig CHECK_JOB_CONFIG = new JobConfig()
      .withConfigType(ConfigType.CHECK_CONNECTION_DESTINATION)
      .withGetSpec(new JobGetSpecConfig());
  private static final JobConfig SYNC_JOB_CONFIG = new JobConfig()
      .withConfigType(ConfigType.SYNC)
      .withSync(new JobSyncConfig());

  private static final JobConfig RESET_JOB_CONFIG = new JobConfig()
      .withConfigType(ConfigType.RESET_CONNECTION)
      .withSync(new JobSyncConfig());

  private static PostgreSQLContainer<?> container;
  private Database jobDatabase;
  private Supplier<Instant> timeSupplier;
  private DefaultJobPersistence jobPersistence;
  private DataSource dataSource;
  private DSLContext dslContext;

  @BeforeAll
  static void dbSetup() {
    container = new PostgreSQLContainer<>(DatabaseConstants.DEFAULT_DATABASE_VERSION)
        .withDatabaseName("airbyte")
        .withUsername("docker")
        .withPassword("docker");
    container.start();
  }

  @AfterAll
  static void dbDown() {
    container.close();
  }

  private static Attempt createAttempt(final int id, final long jobId, final AttemptStatus status, final Path logPath) {
    return new Attempt(
        id,
        jobId,
        logPath,
        null,
        null,
        status,
        null,
        null,
        NOW.getEpochSecond(),
        NOW.getEpochSecond(),
        NOW.getEpochSecond());
  }

  private static Attempt createUnfinishedAttempt(final int id, final long jobId, final AttemptStatus status, final Path logPath) {
    return new Attempt(
        id,
        jobId,
        logPath,
        null,
        null,
        status,
        null,
        null,
        NOW.getEpochSecond(),
        NOW.getEpochSecond(),
        null);
  }

  private static Job createJob(final long id,
                               final JobConfig jobConfig,
                               final JobStatus status,
                               final List<Attempt> attempts,
                               final long time,
                               final boolean isScheduled) {
    return createJob(id, jobConfig, status, attempts, time, SCOPE, isScheduled);
  }

  private static Job createJob(final long id, final JobConfig jobConfig, final JobStatus status, final List<Attempt> attempts, final long time) {
    return createJob(id, jobConfig, status, attempts, time, true);
  }

  private static Job createJob(
                               final long id,
                               final JobConfig jobConfig,
                               final JobStatus status,
                               final List<Attempt> attempts,
                               final long time,
                               final String scope,
                               final boolean isScheduled) {
    return new Job(
        id,
        jobConfig.getConfigType(),
        scope,
        jobConfig,
        attempts,
        status,
        null,
        time,
        time,
        isScheduled);
  }

  private static Supplier<Instant> incrementingSecondSupplier(final Instant startTime) {
    // needs to be an array to work with lambda
    final int[] intArray = {0};

    final Supplier<Instant> timeSupplier = () -> startTime.plusSeconds(intArray[0]++);
    return timeSupplier;
  }

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setup() throws Exception {
    dataSource = Databases.createDataSource(container);
    dslContext = DSLContextFactory.create(dataSource, SQLDialect.POSTGRES);
    final TestDatabaseProviders databaseProviders = new TestDatabaseProviders(dataSource, dslContext);
    jobDatabase = databaseProviders.createNewJobsDatabase();
    resetDb();

    timeSupplier = mock(Supplier.class);
    when(timeSupplier.get()).thenReturn(NOW);

    jobPersistence = new DefaultJobPersistence(jobDatabase, timeSupplier);
  }

  @AfterEach
  void tearDown() throws Exception {
    DataSourceFactory.close(dataSource);
  }

  private void resetDb() throws SQLException {
    // todo (cgardens) - truncate whole db.
    jobDatabase.query(ctx -> ctx.truncateTable(JOBS).cascade().execute());
    jobDatabase.query(ctx -> ctx.truncateTable(ATTEMPTS).cascade().execute());
    jobDatabase.query(ctx -> ctx.truncateTable(AIRBYTE_METADATA).cascade().execute());
    jobDatabase.query(ctx -> ctx.truncateTable(SYNC_STATS));
    jobDatabase.query(ctx -> ctx.truncateTable(STREAM_ATTEMPT_METADATA));
  }

  private Result<Record> getJobRecord(final long jobId) throws SQLException {
    return jobDatabase.query(ctx -> ctx.fetch(DefaultJobPersistence.BASE_JOB_SELECT_AND_JOIN + "WHERE jobs.id = ?", jobId));
  }

  @Test
  @DisplayName("Should set a job to incomplete if an attempt fails")
  void testCompleteAttemptFailed() throws IOException {
    final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
    final int attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH);

    jobPersistence.failAttempt(jobId, attemptNumber);

    final Job actual = jobPersistence.getJob(jobId);
    final Job expected = createJob(
        jobId,
        SPEC_JOB_CONFIG,
        JobStatus.INCOMPLETE,
        Lists.newArrayList(createAttempt(0, jobId, AttemptStatus.FAILED, LOG_PATH)),
        NOW.getEpochSecond());
    assertEquals(expected, actual);
  }

  @Test
  @DisplayName("Should set a job to succeeded if an attempt succeeds")
  void testCompleteAttemptSuccess() throws IOException {
    final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
    final int attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH);

    jobPersistence.succeedAttempt(jobId, attemptNumber);

    final Job actual = jobPersistence.getJob(jobId);
    final Job expected = createJob(
        jobId,
        SPEC_JOB_CONFIG,
        JobStatus.SUCCEEDED,
        Lists.newArrayList(createAttempt(0, jobId, AttemptStatus.SUCCEEDED, LOG_PATH)),
        NOW.getEpochSecond());
    assertEquals(expected, actual);
  }

  @Test
  @DisplayName("Should be able to read what is written")
  void testWriteOutput() throws IOException {
    final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
    final int attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH);
    final Job created = jobPersistence.getJob(jobId);
    final SyncStats syncStats =
        new SyncStats().withBytesEmitted(100L).withRecordsEmitted(9L).withRecordsCommitted(10L).withDestinationStateMessagesEmitted(1L)
            .withSourceStateMessagesEmitted(4L).withMaxSecondsBeforeSourceStateMessageEmitted(5L).withMeanSecondsBeforeSourceStateMessageEmitted(2L)
            .withMaxSecondsBetweenStateMessageEmittedandCommitted(10L).withMeanSecondsBetweenStateMessageEmittedandCommitted(3L);
    final String streamName = "stream";
    final String streamNamespace = "namespace";
    final StreamSyncStats streamSyncStats = new StreamSyncStats().withStats(
        new SyncStats().withBytesEmitted(100L).withRecordsEmitted(9L).withEstimatedBytes(200L).withEstimatedRecords(10L))
        .withStreamNamespace(streamNamespace).withStreamName(streamName);

    final StandardSyncOutput standardSyncOutput =
        new StandardSyncOutput().withStandardSyncSummary(new StandardSyncSummary()
            .withTotalStats(syncStats)
            .withStreamStats(List.of(streamSyncStats)));
    final JobOutput jobOutput = new JobOutput().withOutputType(JobOutput.OutputType.DISCOVER_CATALOG).withSync(standardSyncOutput);

    when(timeSupplier.get()).thenReturn(Instant.ofEpochMilli(4242));
    jobPersistence.writeOutput(jobId, attemptNumber, jobOutput);

    final Job updated = jobPersistence.getJob(jobId);

    assertEquals(Optional.of(jobOutput), updated.getAttempts().get(0).getOutput());
    assertNotEquals(created.getAttempts().get(0).getUpdatedAtInSecond(), updated.getAttempts().get(0).getUpdatedAtInSecond());

    final AttemptStats attemptStats = jobPersistence.getAttemptStats(jobId, attemptNumber);

    final SyncStats storedSyncStats = attemptStats.combinedStats();
    assertEquals(100L, storedSyncStats.getBytesEmitted());
    assertEquals(9L, storedSyncStats.getRecordsEmitted());
    assertEquals(10L, storedSyncStats.getRecordsCommitted());
    assertEquals(4L, storedSyncStats.getSourceStateMessagesEmitted());
    assertEquals(1L, storedSyncStats.getDestinationStateMessagesEmitted());
    assertEquals(5L, storedSyncStats.getMaxSecondsBeforeSourceStateMessageEmitted());
    assertEquals(2L, storedSyncStats.getMeanSecondsBeforeSourceStateMessageEmitted());
    assertEquals(10L, storedSyncStats.getMaxSecondsBetweenStateMessageEmittedandCommitted());
    assertEquals(3L, storedSyncStats.getMeanSecondsBetweenStateMessageEmittedandCommitted());

    final List<StreamSyncStats> storedStreamSyncStats = attemptStats.perStreamStats();
    assertEquals(1, storedStreamSyncStats.size());
    assertEquals(streamName, storedStreamSyncStats.get(0).getStreamName());
    assertEquals(streamNamespace, storedStreamSyncStats.get(0).getStreamNamespace());
    assertEquals(streamSyncStats.getStats().getBytesEmitted(), storedStreamSyncStats.get(0).getStats().getBytesEmitted());
    assertEquals(streamSyncStats.getStats().getRecordsEmitted(), storedStreamSyncStats.get(0).getStats().getRecordsEmitted());
    assertEquals(streamSyncStats.getStats().getEstimatedRecords(), storedStreamSyncStats.get(0).getStats().getEstimatedRecords());
    assertEquals(streamSyncStats.getStats().getEstimatedBytes(), storedStreamSyncStats.get(0).getStats().getEstimatedBytes());
  }

  @Test
  @DisplayName("Should be able to read AttemptSyncConfig that was written")
  void testWriteAttemptSyncConfig() throws IOException {
    final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
    final int attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH);
    final Job created = jobPersistence.getJob(jobId);
    final AttemptSyncConfig attemptSyncConfig = new AttemptSyncConfig()
        .withSourceConfiguration(Jsons.jsonNode(Map.of("source", "s_config_value")))
        .withDestinationConfiguration(Jsons.jsonNode(Map.of("destination", "d_config_value")))
        .withState(new State().withState(Jsons.jsonNode(ImmutableMap.of("state_key", "state_value"))));

    when(timeSupplier.get()).thenReturn(Instant.ofEpochMilli(4242));
    jobPersistence.writeAttemptSyncConfig(jobId, attemptNumber, attemptSyncConfig);

    final Job updated = jobPersistence.getJob(jobId);
    assertEquals(Optional.of(attemptSyncConfig), updated.getAttempts().get(0).getSyncConfig());
    assertNotEquals(created.getAttempts().get(0).getUpdatedAtInSecond(), updated.getAttempts().get(0).getUpdatedAtInSecond());
  }

  @Test
  @DisplayName("Should be able to read attemptFailureSummary that was written")
  void testWriteAttemptFailureSummary() throws IOException {
    final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
    final int attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH);
    final Job created = jobPersistence.getJob(jobId);
    final AttemptFailureSummary failureSummary = new AttemptFailureSummary().withFailures(
        Collections.singletonList(new FailureReason().withFailureOrigin(FailureOrigin.SOURCE)));

    when(timeSupplier.get()).thenReturn(Instant.ofEpochMilli(4242));
    jobPersistence.writeAttemptFailureSummary(jobId, attemptNumber, failureSummary);

    final Job updated = jobPersistence.getJob(jobId);
    assertEquals(Optional.of(failureSummary), updated.getAttempts().get(0).getFailureSummary());
    assertNotEquals(created.getAttempts().get(0).getUpdatedAtInSecond(), updated.getAttempts().get(0).getUpdatedAtInSecond());
  }

  @Test
  @DisplayName("Should be able to read attemptFailureSummary that was written with unsupported unicode")
  void testWriteAttemptFailureSummaryWithUnsupportedUnicode() throws IOException {
    final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
    final int attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH);
    final Job created = jobPersistence.getJob(jobId);
    final AttemptFailureSummary failureSummary = new AttemptFailureSummary().withFailures(
        Collections.singletonList(new FailureReason().withFailureOrigin(FailureOrigin.SOURCE)
            .withStacktrace(Character.toString(0))
            .withInternalMessage("Includes invalid unicode \u0000")
            .withExternalMessage("Includes invalid unicode \0")));
    when(timeSupplier.get()).thenReturn(Instant.ofEpochMilli(4242));
    jobPersistence.writeAttemptFailureSummary(jobId, attemptNumber, failureSummary);

    Assertions.assertDoesNotThrow(() -> {
      final Job updated = jobPersistence.getJob(jobId);
      assertTrue(updated.getAttempts().get(0).getFailureSummary().isPresent());
      assertNotEquals(created.getAttempts().get(0).getUpdatedAtInSecond(), updated.getAttempts().get(0).getUpdatedAtInSecond());
    });
  }

  @Test
  @DisplayName("When getting the last replication job should return the most recently created job")
  void testGetLastSyncJobWithMultipleAttempts() throws IOException {
    final long jobId = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow();
    jobPersistence.failAttempt(jobId, jobPersistence.createAttempt(jobId, LOG_PATH));
    jobPersistence.failAttempt(jobId, jobPersistence.createAttempt(jobId, LOG_PATH));

    final Optional<Job> actual = jobPersistence.getLastReplicationJob(UUID.fromString(SCOPE));

    final Job expected = createJob(
        jobId,
        SYNC_JOB_CONFIG,
        JobStatus.INCOMPLETE,
        Lists.newArrayList(
            createAttempt(0, jobId, AttemptStatus.FAILED, LOG_PATH),
            createAttempt(1, jobId, AttemptStatus.FAILED, LOG_PATH)),
        NOW.getEpochSecond());

    assertEquals(Optional.of(expected), actual);
  }

  @Test
  @DisplayName("When getting the last replication job should return the most recently created job without scheduled functionalities")
  void testGetLastSyncJobWithCancelWithScheduleEnabledForNonScheduledJob() throws IOException {
    final long jobId = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, false).orElseThrow();
    jobPersistence.failAttempt(jobId, jobPersistence.createAttempt(jobId, LOG_PATH));
    jobPersistence.failAttempt(jobId, jobPersistence.createAttempt(jobId, LOG_PATH));
    jobPersistence.cancelJob(jobId);

    final Optional<Job> actual = jobPersistence.getLastReplicationJobWithCancel(UUID.fromString(SCOPE));

    assertEquals(Optional.empty(), actual);
  }

  @Test
  @DisplayName("When getting the last replication job should return the most recently created job with scheduled functionalities")
  void testGetLastSyncJobWithCancelWithScheduleEnabledForScheduledJob() throws IOException {
    final long jobId = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow();
    jobPersistence.failAttempt(jobId, jobPersistence.createAttempt(jobId, LOG_PATH));
    jobPersistence.failAttempt(jobId, jobPersistence.createAttempt(jobId, LOG_PATH));
    jobPersistence.cancelJob(jobId);

    final Optional<Job> actual = jobPersistence.getLastReplicationJobWithCancel(UUID.fromString(SCOPE));

    final Job expected = createJob(
        jobId,
        SYNC_JOB_CONFIG,
        JobStatus.CANCELLED,
        Lists.newArrayList(
            createAttempt(0, jobId, AttemptStatus.FAILED, LOG_PATH),
            createAttempt(1, jobId, AttemptStatus.FAILED, LOG_PATH)),
        NOW.getEpochSecond(),
        true);
    assertEquals(Optional.of(expected), actual);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @DisplayName("Should extract a Job model from a JOOQ result set")
  void testGetJobFromRecord(final boolean isScheduled) throws IOException, SQLException {
    final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, isScheduled).orElseThrow();

    final Optional<Job> actual = DefaultJobPersistence.getJobFromResult(getJobRecord(jobId));

    final Job expected = createJob(jobId, SPEC_JOB_CONFIG, JobStatus.PENDING, Collections.emptyList(), NOW.getEpochSecond(), isScheduled);
    assertEquals(Optional.of(expected), actual);
  }

  @Test
  @DisplayName("Should extract a Job model from a JOOQ result set")
  void testGetJobFromRecordDefaultIsScheduled() throws IOException, SQLException {
    final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();

    final Optional<Job> actual = DefaultJobPersistence.getJobFromResult(getJobRecord(jobId));

    final Job expected = createJob(jobId, SPEC_JOB_CONFIG, JobStatus.PENDING, Collections.emptyList(), NOW.getEpochSecond(), true);
    assertEquals(Optional.of(expected), actual);
  }

  @Test
  @DisplayName("Should return correct set of jobs when querying on end timestamp")
  void testListJobsWithTimestamp() throws IOException {
    // TODO : Once we fix the problem of precision loss in DefaultJobPersistence, change the test value
    // to contain milliseconds as well
    final Instant now = Instant.parse("2021-01-01T00:00:00Z");
    final Supplier<Instant> timeSupplier = incrementingSecondSupplier(now);

    jobPersistence = new DefaultJobPersistence(jobDatabase, timeSupplier);
    final long syncJobId = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow();
    final int syncJobAttemptNumber0 = jobPersistence.createAttempt(syncJobId, LOG_PATH);
    jobPersistence.failAttempt(syncJobId, syncJobAttemptNumber0);
    final Path syncJobSecondAttemptLogPath = LOG_PATH.resolve("2");
    final int syncJobAttemptNumber1 = jobPersistence.createAttempt(syncJobId, syncJobSecondAttemptLogPath);
    jobPersistence.failAttempt(syncJobId, syncJobAttemptNumber1);

    final long specJobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
    final int specJobAttemptNumber0 = jobPersistence.createAttempt(specJobId, LOG_PATH);
    jobPersistence.failAttempt(specJobId, specJobAttemptNumber0);
    final Path specJobSecondAttemptLogPath = LOG_PATH.resolve("2");
    final int specJobAttemptNumber1 = jobPersistence.createAttempt(specJobId, specJobSecondAttemptLogPath);
    jobPersistence.succeedAttempt(specJobId, specJobAttemptNumber1);

    final List<Job> jobs = jobPersistence.listJobs(ConfigType.SYNC, Instant.EPOCH);
    assertEquals(jobs.size(), 1);
    assertEquals(jobs.get(0).getId(), syncJobId);
    assertEquals(jobs.get(0).getAttempts().size(), 2);
    assertEquals(jobs.get(0).getAttempts().get(0).getAttemptNumber(), 0);
    assertEquals(jobs.get(0).getAttempts().get(1).getAttemptNumber(), 1);

    final Path syncJobThirdAttemptLogPath = LOG_PATH.resolve("3");
    final int syncJobAttemptNumber2 = jobPersistence.createAttempt(syncJobId, syncJobThirdAttemptLogPath);
    jobPersistence.succeedAttempt(syncJobId, syncJobAttemptNumber2);

    final long newSyncJobId = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow();
    final int newSyncJobAttemptNumber0 = jobPersistence.createAttempt(newSyncJobId, LOG_PATH);
    jobPersistence.failAttempt(newSyncJobId, newSyncJobAttemptNumber0);
    final Path newSyncJobSecondAttemptLogPath = LOG_PATH.resolve("2");
    final int newSyncJobAttemptNumber1 = jobPersistence.createAttempt(newSyncJobId, newSyncJobSecondAttemptLogPath);
    jobPersistence.succeedAttempt(newSyncJobId, newSyncJobAttemptNumber1);

    final Long maxEndedAtTimestamp =
        jobs.get(0).getAttempts().stream().map(c -> c.getEndedAtInSecond().orElseThrow()).max(Long::compareTo).orElseThrow();

    final List<Job> secondQueryJobs = jobPersistence.listJobs(ConfigType.SYNC, Instant.ofEpochSecond(maxEndedAtTimestamp));
    assertEquals(secondQueryJobs.size(), 2);
    assertEquals(secondQueryJobs.get(0).getId(), syncJobId);
    assertEquals(secondQueryJobs.get(0).getAttempts().size(), 1);
    assertEquals(secondQueryJobs.get(0).getAttempts().get(0).getAttemptNumber(), 2);

    assertEquals(secondQueryJobs.get(1).getId(), newSyncJobId);
    assertEquals(secondQueryJobs.get(1).getAttempts().size(), 2);
    assertEquals(secondQueryJobs.get(1).getAttempts().get(0).getAttemptNumber(), 0);
    assertEquals(secondQueryJobs.get(1).getAttempts().get(1).getAttemptNumber(), 1);

    Long maxEndedAtTimestampAfterSecondQuery = -1L;
    for (final Job c : secondQueryJobs) {
      final List<Attempt> attempts = c.getAttempts();
      final Long maxEndedAtTimestampForJob = attempts.stream().map(attempt -> attempt.getEndedAtInSecond().orElseThrow())
          .max(Long::compareTo).orElseThrow();
      if (maxEndedAtTimestampForJob > maxEndedAtTimestampAfterSecondQuery) {
        maxEndedAtTimestampAfterSecondQuery = maxEndedAtTimestampForJob;
      }
    }

    assertEquals(0, jobPersistence.listJobs(ConfigType.SYNC, Instant.ofEpochSecond(maxEndedAtTimestampAfterSecondQuery)).size());
  }

  @Test
  void testAirbyteProtocolVersionMaxMetadata() throws IOException {
    assertTrue(jobPersistence.getAirbyteProtocolVersionMax().isEmpty());

    final Version maxVersion1 = new Version("0.1.0");
    jobPersistence.setAirbyteProtocolVersionMax(maxVersion1);
    final Optional<Version> maxVersion1read = jobPersistence.getAirbyteProtocolVersionMax();
    assertEquals(maxVersion1, maxVersion1read.orElseThrow());

    final Version maxVersion2 = new Version("1.2.1");
    jobPersistence.setAirbyteProtocolVersionMax(maxVersion2);
    final Optional<Version> maxVersion2read = jobPersistence.getAirbyteProtocolVersionMax();
    assertEquals(maxVersion2, maxVersion2read.orElseThrow());
  }

  @Test
  void testAirbyteProtocolVersionMinMetadata() throws IOException {
    assertTrue(jobPersistence.getAirbyteProtocolVersionMin().isEmpty());

    final Version minVersion1 = new Version("1.1.0");
    jobPersistence.setAirbyteProtocolVersionMin(minVersion1);
    final Optional<Version> minVersion1read = jobPersistence.getAirbyteProtocolVersionMin();
    assertEquals(minVersion1, minVersion1read.orElseThrow());

    final Version minVersion2 = new Version("3.0.1");
    jobPersistence.setAirbyteProtocolVersionMin(minVersion2);
    final Optional<Version> minVersion2read = jobPersistence.getAirbyteProtocolVersionMin();
    assertEquals(minVersion2, minVersion2read.orElseThrow());
  }

  @Test
  void testAirbyteProtocolVersionRange() throws IOException {
    final Version v1 = new Version("1.5.0");
    final Version v2 = new Version("2.5.0");
    final Optional<AirbyteProtocolVersionRange> range = jobPersistence.getCurrentProtocolVersionRange();
    assertEquals(Optional.empty(), range);

    jobPersistence.setAirbyteProtocolVersionMax(v2);
    final Optional<AirbyteProtocolVersionRange> range2 = jobPersistence.getCurrentProtocolVersionRange();
    assertEquals(Optional.of(new AirbyteProtocolVersionRange(AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION, v2)), range2);

    jobPersistence.setAirbyteProtocolVersionMin(v1);
    final Optional<AirbyteProtocolVersionRange> range3 = jobPersistence.getCurrentProtocolVersionRange();
    assertEquals(Optional.of(new AirbyteProtocolVersionRange(v1, v2)), range3);
  }

  @Nested
  @DisplayName("Stats Related Tests")
  class Stats {

    @Test
    @DisplayName("Writing stats the first time should only write record and bytes information correctly")
    void testWriteStatsFirst() throws IOException {
      final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
      final int attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH);
      final var streamStats = List.of(
          new StreamSyncStats().withStreamName("name1").withStreamNamespace("ns")
              .withStats(new SyncStats().withBytesEmitted(500L).withRecordsEmitted(500L).withEstimatedBytes(10000L).withEstimatedRecords(2000L)),
          new StreamSyncStats().withStreamName("name2").withStreamNamespace("ns")
              .withStats(new SyncStats().withBytesEmitted(500L).withRecordsEmitted(500L).withEstimatedBytes(10000L).withEstimatedRecords(2000L)));
      final long estimatedRecords = 1000L;
      final long estimatedBytes = 1001L;
      final long recordsEmitted = 1002L;
      final long bytesEmitted = 1003L;
      final long recordsCommitted = 1004L;
      final long bytesCommitted = 1005L;
      jobPersistence.writeStats(jobId, attemptNumber, estimatedRecords, estimatedBytes, recordsEmitted, bytesEmitted, recordsCommitted,
          bytesCommitted, CONNECTION_ID, streamStats);

      final AttemptStats stats = jobPersistence.getAttemptStats(jobId, attemptNumber);
      final var combined = stats.combinedStats();
      assertEquals(bytesEmitted, combined.getBytesEmitted());
      assertEquals(recordsEmitted, combined.getRecordsEmitted());
      assertEquals(estimatedBytes, combined.getEstimatedBytes());
      assertEquals(estimatedRecords, combined.getEstimatedRecords());
      assertEquals(recordsCommitted, combined.getRecordsCommitted());
      assertEquals(bytesCommitted, combined.getBytesCommitted());

      // As of this writing, committed and state messages are not expected.
      assertNull(combined.getDestinationStateMessagesEmitted());

      final var actStreamStats = stats.perStreamStats();
      assertEquals(2, actStreamStats.size());
      assertEquals(streamStats, actStreamStats);
    }

    @Test
    @DisplayName("Fetch stream stats get the default metadata correctly")
    void testWriteStatsWithMetadataDefault() throws IOException {
      final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
      final int attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH);
      final var streamStats = List.of(
          new StreamSyncStats().withStreamName("name1").withStreamNamespace("ns")
              .withStats(new SyncStats().withBytesEmitted(500L).withRecordsEmitted(500L).withEstimatedBytes(10000L).withEstimatedRecords(2000L))
              .withWasResumed(false).withWasBackfilled(false),
          new StreamSyncStats().withStreamName("name2").withStreamNamespace("ns")
              .withStats(new SyncStats().withBytesEmitted(500L).withRecordsEmitted(500L).withEstimatedBytes(10000L).withEstimatedRecords(2000L))
              .withWasResumed(false).withWasBackfilled(false));
      final long estimatedRecords = 1000L;
      final long estimatedBytes = 1001L;
      final long recordsEmitted = 1002L;
      final long bytesEmitted = 1003L;
      final long recordsCommitted = 1004L;
      final long bytesCommitted = 1005L;
      jobPersistence.writeStats(jobId, attemptNumber, estimatedRecords, estimatedBytes, recordsEmitted, bytesEmitted, recordsCommitted,
          bytesCommitted, CONNECTION_ID, streamStats);

      final AttemptStats stats = jobPersistence.getAttemptStatsWithStreamMetadata(jobId, attemptNumber);

      final var actStreamStats = stats.perStreamStats();
      assertEquals(2, actStreamStats.size());
      assertEquals(streamStats, actStreamStats);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("Fetch stream stats get the metadata correctly")
    void testWriteStatsWithMetadata(final boolean resumedBackfilledValue) throws IOException, SQLException {
      final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
      final int attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH);
      final var streamStats = List.of(
          new StreamSyncStats().withStreamName("name1")
              .withStats(new SyncStats().withBytesEmitted(500L).withRecordsEmitted(500L).withEstimatedBytes(10000L).withEstimatedRecords(2000L))
              .withWasResumed(resumedBackfilledValue).withWasBackfilled(resumedBackfilledValue),
          new StreamSyncStats().withStreamName("name2")
              .withStats(new SyncStats().withBytesEmitted(500L).withRecordsEmitted(500L).withEstimatedBytes(10000L).withEstimatedRecords(2000L))
              .withWasResumed(resumedBackfilledValue).withWasBackfilled(resumedBackfilledValue));
      final long estimatedRecords = 1000L;
      final long estimatedBytes = 1001L;
      final long recordsEmitted = 1002L;
      final long bytesEmitted = 1003L;
      final long recordsCommitted = 1004L;
      final long bytesCommitted = 1005L;
      jobPersistence.writeStats(jobId, attemptNumber, estimatedRecords, estimatedBytes, recordsEmitted, bytesEmitted, recordsCommitted,
          bytesCommitted, CONNECTION_ID, streamStats);

      final long attemptId = jobDatabase.query(
          ctx -> ctx.select(ATTEMPTS.ID).from(ATTEMPTS).where(ATTEMPTS.JOB_ID.eq(jobId).and(ATTEMPTS.ATTEMPT_NUMBER.eq(attemptNumber)))
              .fetchOne(ATTEMPTS.ID));

      jobDatabase.query(
          ctx -> ctx.insertInto(
              STREAM_ATTEMPT_METADATA,
              STREAM_ATTEMPT_METADATA.ID,
              STREAM_ATTEMPT_METADATA.ATTEMPT_ID,
              STREAM_ATTEMPT_METADATA.STREAM_NAME,
              STREAM_ATTEMPT_METADATA.STREAM_NAMESPACE,
              STREAM_ATTEMPT_METADATA.WAS_BACKFILLED,
              STREAM_ATTEMPT_METADATA.WAS_RESUMED)
              .values(UUID.randomUUID(), attemptId, "name1", null, resumedBackfilledValue, resumedBackfilledValue)
              .values(UUID.randomUUID(), attemptId, "name2", null, resumedBackfilledValue, resumedBackfilledValue)
              .execute());

      final AttemptStats stats = jobPersistence.getAttemptStatsWithStreamMetadata(jobId, attemptNumber);

      final var actStreamStats = stats.perStreamStats();
      assertEquals(2, actStreamStats.size());
      assertEquals(streamStats, actStreamStats);
    }

    @Test
    @DisplayName("Writing stats multiple times should write record and bytes information correctly without exceptions")
    void testWriteStatsRepeated() throws IOException {
      final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
      final int attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH);

      // First write.
      var streamStats = List.of(
          new StreamSyncStats().withStreamName("name1").withStreamNamespace("ns")
              .withStats(new SyncStats().withBytesEmitted(500L).withRecordsEmitted(500L).withEstimatedBytes(10000L).withEstimatedRecords(2000L)));
      jobPersistence.writeStats(jobId, attemptNumber, 1000L, 1000L, 1000L, 1000L, 1000L, 1000L, CONNECTION_ID, streamStats);

      // Second write.
      when(timeSupplier.get()).thenReturn(Instant.now());
      streamStats = List.of(
          new StreamSyncStats().withStreamName("name1").withStreamNamespace("ns")
              .withStats(new SyncStats().withBytesEmitted(1000L).withRecordsEmitted(1000L).withEstimatedBytes(10000L).withEstimatedRecords(2000L)));
      jobPersistence.writeStats(jobId, attemptNumber, 2000L, 2000L, 2000L, 2000L, 2000L, 2000L, CONNECTION_ID, streamStats);

      final AttemptStats stats = jobPersistence.getAttemptStats(jobId, attemptNumber);
      final var combined = stats.combinedStats();
      assertEquals(2000, combined.getBytesEmitted());
      assertEquals(2000, combined.getRecordsEmitted());
      assertEquals(2000, combined.getEstimatedBytes());
      assertEquals(2000, combined.getEstimatedRecords());

      final var actStreamStats = stats.perStreamStats();
      assertEquals(1, actStreamStats.size());
      assertEquals(streamStats, actStreamStats);

    }

    @Test
    @DisplayName("Writing multiple stats of the same attempt id, stream name and namespace should update the previous record")
    void testWriteStatsUpsert() throws IOException, SQLException {
      final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
      final int attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH);

      // First write.
      var streamStats = List.of(
          new StreamSyncStats().withStreamName("name1").withStreamNamespace("ns")
              .withStats(new SyncStats().withBytesEmitted(500L).withRecordsEmitted(500L).withEstimatedBytes(10000L).withEstimatedRecords(2000L)));
      jobPersistence.writeStats(jobId, attemptNumber, 1000L, 1000L, 1000L, 1000L, 1000L, 1000L, CONNECTION_ID, streamStats);

      // Second write.
      when(timeSupplier.get()).thenReturn(Instant.now());
      streamStats = List.of(
          new StreamSyncStats().withStreamName("name1").withStreamNamespace("ns")
              .withStats(new SyncStats().withBytesEmitted(1000L).withRecordsEmitted(1000L).withEstimatedBytes(10000L).withEstimatedRecords(2000L)));
      jobPersistence.writeStats(jobId, attemptNumber, 2000L, 2000L, 2000L, 2000L, 2000L, 2000L, CONNECTION_ID, streamStats);

      final var syncStatsRec = jobDatabase.query(ctx -> {
        final var attemptId = DefaultJobPersistence.getAttemptId(jobId, attemptNumber, ctx);
        return ctx.fetch("SELECT * from sync_stats where attempt_id = ?", attemptId).stream().findFirst().get();
      });

      // Check time stamps to confirm upsert.
      assertNotEquals(syncStatsRec.get(SYNC_STATS.CREATED_AT), syncStatsRec.get(SYNC_STATS.UPDATED_AT));

      final var streamStatsRec = jobDatabase.query(ctx -> {
        final var attemptId = DefaultJobPersistence.getAttemptId(jobId, attemptNumber, ctx);
        return ctx.fetch("SELECT * from stream_stats where attempt_id = ?", attemptId).stream().findFirst().get();
      });
      // Check time stamps to confirm upsert.
      assertNotEquals(streamStatsRec.get(STREAM_STATS.CREATED_AT), streamStatsRec.get(STREAM_STATS.UPDATED_AT));
    }

    @Test
    @DisplayName("Writing multiple stats a stream with null namespace should write correctly without exceptions")
    void testWriteNullNamespace() throws IOException {
      final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
      final int attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH);

      // First write.
      var streamStats = List.of(
          new StreamSyncStats().withStreamName("name1")
              .withStats(new SyncStats().withBytesEmitted(500L).withRecordsEmitted(500L).withEstimatedBytes(10000L).withEstimatedRecords(2000L)));
      jobPersistence.writeStats(jobId, attemptNumber, 1000L, 1000L, 1000L, 1000L, 1000L, 1000L, CONNECTION_ID, streamStats);

      // Second write.
      when(timeSupplier.get()).thenReturn(Instant.now());
      streamStats = List.of(
          new StreamSyncStats().withStreamName("name1")
              .withStats(new SyncStats().withBytesEmitted(1000L).withRecordsEmitted(1000L).withEstimatedBytes(10000L).withEstimatedRecords(2000L)));
      jobPersistence.writeStats(jobId, attemptNumber, 2000L, 2000L, 2000L, 2000L, 2000L, 2000L, CONNECTION_ID, streamStats);

      final AttemptStats stats = jobPersistence.getAttemptStats(jobId, attemptNumber);
      final var combined = stats.combinedStats();
      assertEquals(2000, combined.getBytesEmitted());
      assertEquals(2000, combined.getRecordsEmitted());
      assertEquals(2000, combined.getEstimatedBytes());
      assertEquals(2000, combined.getEstimatedRecords());

      final var actStreamStats = stats.perStreamStats();
      assertEquals(1, actStreamStats.size());
      assertEquals(streamStats, actStreamStats);
    }

    @Test
    @DisplayName("Writing multiple stats a stream with null namespace should write correctly without exceptions")
    void testGetStatsNoResult() throws IOException {
      final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
      final int attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH);

      final AttemptStats stats = jobPersistence.getAttemptStats(jobId, attemptNumber);
      assertNull(stats.combinedStats());
      assertEquals(0, stats.perStreamStats().size());

    }

    @Test
    @DisplayName("Retrieving all attempts stats for a job should return the right information")
    void testGetMultipleStats() throws IOException, SQLException {
      final long jobOneId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
      final int jobOneAttemptNumberOne = jobPersistence.createAttempt(jobOneId, LOG_PATH);

      // First write for first attempt.
      var streamStats = List.of(
          new StreamSyncStats().withStreamName("name1")
              .withStats(new SyncStats()
                  .withBytesEmitted(1L).withRecordsEmitted(1L)
                  .withEstimatedBytes(2L).withEstimatedRecords(2L)),
          new StreamSyncStats().withStreamName("name2").withStreamNamespace("ns")
              .withStats(new SyncStats()
                  .withBytesEmitted(1L).withRecordsEmitted(1L)
                  .withEstimatedBytes(2L).withEstimatedRecords(2L)));
      jobPersistence.writeStats(jobOneId, jobOneAttemptNumberOne, 1000L, 1000L, 1000L, 1000L, 1000L, 1000L, CONNECTION_ID, streamStats);

      // Second write for first attempt. This is the record that should be returned.
      when(timeSupplier.get()).thenReturn(Instant.now());
      streamStats = List.of(
          new StreamSyncStats().withStreamName("name1")
              .withStats(new SyncStats()
                  .withBytesEmitted(100L).withRecordsEmitted(10L)
                  .withEstimatedBytes(200L).withEstimatedRecords(20L)
                  .withBytesCommitted(100L).withRecordsCommitted(10L)),
          new StreamSyncStats().withStreamName("name2").withStreamNamespace("ns")
              .withStats(new SyncStats()
                  .withBytesEmitted(1000L).withRecordsEmitted(100L)
                  .withEstimatedBytes(2000L).withEstimatedRecords(200L)
                  .withBytesCommitted(888L).withRecordsCommitted(88L)));
      jobPersistence.writeStats(jobOneId, jobOneAttemptNumberOne, 220L, 2200L, 110L, 1100L, 98L, 988L, CONNECTION_ID, streamStats);
      jobPersistence.failAttempt(jobOneId, jobOneAttemptNumberOne);

      // Second attempt for first job.
      streamStats = List.of(
          new StreamSyncStats().withStreamName("name1")
              .withStats(new SyncStats()
                  .withBytesEmitted(1000L).withRecordsEmitted(100L)
                  .withEstimatedBytes(2000L).withEstimatedRecords(200L)
                  .withBytesCommitted(1000L).withRecordsCommitted(100L)),
          new StreamSyncStats().withStreamName("name2").withStreamNamespace("ns")
              .withStats(new SyncStats()
                  .withBytesEmitted(10000L).withRecordsEmitted(1000L)
                  .withEstimatedBytes(20000L).withEstimatedRecords(2000L)
                  .withBytesCommitted(8880L).withRecordsCommitted(880L)));
      final int jobOneAttemptNumberTwo = jobPersistence.createAttempt(jobOneId, LOG_PATH);
      jobPersistence.writeStats(jobOneId, jobOneAttemptNumberTwo, 1000L, 1000L, 1000L, 1000L, 1000L, 1000L, CONNECTION_ID, streamStats);

      // First attempt for second job.
      final long jobTwoId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
      final int jobTwoAttemptNumberOne = jobPersistence.createAttempt(jobTwoId, LOG_PATH);
      streamStats = List.of(
          new StreamSyncStats().withStreamName("name1")
              .withStats(new SyncStats()
                  .withBytesEmitted(1000L).withRecordsEmitted(1000L)
                  .withEstimatedBytes(10000L).withEstimatedRecords(2000L)),
          new StreamSyncStats().withStreamName("name2").withStreamNamespace("ns")
              .withStats(new SyncStats()
                  .withBytesEmitted(5000L).withRecordsEmitted(5000L)
                  .withEstimatedBytes(100000L).withEstimatedRecords(20000L)));
      jobPersistence.writeStats(jobTwoId, jobTwoAttemptNumberOne, 1000L, 1000L, 1000L, 1000L, 1000L, 1000L, CONNECTION_ID, streamStats);

      final List<Long> jobOneAttemptIds = jobDatabase.query(
          ctx -> ctx.select(ATTEMPTS.ID).from(ATTEMPTS).where(ATTEMPTS.JOB_ID.eq(jobOneId)).orderBy(ATTEMPTS.ID).fetch()
              .map(r -> r.get(ATTEMPTS.ID)));
      final List<Long> jobTwoAttemptIds = jobDatabase.query(
          ctx -> ctx.select(ATTEMPTS.ID).from(ATTEMPTS).where(ATTEMPTS.JOB_ID.eq(jobTwoId)).orderBy(ATTEMPTS.ID).fetch()
              .map(r -> r.get(ATTEMPTS.ID)));
      jobDatabase.query(
          ctx -> ctx.insertInto(
              STREAM_ATTEMPT_METADATA,
              STREAM_ATTEMPT_METADATA.ID,
              STREAM_ATTEMPT_METADATA.ATTEMPT_ID,
              STREAM_ATTEMPT_METADATA.STREAM_NAME,
              STREAM_ATTEMPT_METADATA.STREAM_NAMESPACE,
              STREAM_ATTEMPT_METADATA.WAS_BACKFILLED,
              STREAM_ATTEMPT_METADATA.WAS_RESUMED)
              .values(UUID.randomUUID(), jobOneAttemptIds.get(0), "name1", null, true, false)
              .values(UUID.randomUUID(), jobOneAttemptIds.get(1), "name1", null, false, true)
              .values(UUID.randomUUID(), jobTwoAttemptIds.get(0), "name2", "ns", true, false)
              .execute());

      final var stats = jobPersistence.getAttemptStats(List.of(jobOneId, jobTwoId));
      final var exp = Map.of(
          new JobAttemptPair(jobOneId, jobOneAttemptNumberOne),
          new AttemptStats(
              new SyncStats()
                  .withBytesEmitted(1100L).withRecordsEmitted(110L)
                  .withEstimatedBytes(2200L).withEstimatedRecords(220L)
                  .withBytesCommitted(988L).withRecordsCommitted(98L),
              List.of(new StreamSyncStats().withStreamName("name1").withStats(
                  new SyncStats()
                      .withBytesEmitted(100L).withRecordsEmitted(10L)
                      .withEstimatedBytes(200L).withEstimatedRecords(20L)
                      .withBytesCommitted(100L).withRecordsCommitted(10L))
                  .withWasBackfilled(true)
                  .withWasResumed(false),
                  new StreamSyncStats().withStreamName("name2").withStreamNamespace("ns")
                      .withStats(new SyncStats()
                          .withBytesEmitted(1000L).withRecordsEmitted(100L)
                          .withEstimatedBytes(2000L).withEstimatedRecords(200L)
                          .withBytesCommitted(888L).withRecordsCommitted(88L))
                      .withWasBackfilled(false)
                      .withWasResumed(false))),
          new JobAttemptPair(jobOneId, jobOneAttemptNumberTwo),
          new AttemptStats(
              new SyncStats()
                  .withRecordsEmitted(1000L).withBytesEmitted(1000L)
                  .withEstimatedBytes(1000L).withEstimatedRecords(1000L)
                  .withBytesCommitted(1000L).withRecordsCommitted(1000L),
              List.of(new StreamSyncStats().withStreamName("name1").withStats(
                  new SyncStats()
                      .withBytesEmitted(1000L).withRecordsEmitted(100L)
                      .withEstimatedBytes(2000L).withEstimatedRecords(200L)
                      .withBytesCommitted(1000L).withRecordsCommitted(100L))
                  .withWasBackfilled(false)
                  .withWasResumed(true),
                  new StreamSyncStats().withStreamName("name2").withStreamNamespace("ns")
                      .withStats(new SyncStats()
                          .withBytesEmitted(10000L).withRecordsEmitted(1000L)
                          .withEstimatedBytes(20000L).withEstimatedRecords(2000L)
                          .withBytesCommitted(8880L).withRecordsCommitted(880L))
                      .withWasBackfilled(false)
                      .withWasResumed(false))),
          new JobAttemptPair(jobTwoId, jobTwoAttemptNumberOne),
          new AttemptStats(
              new SyncStats()
                  .withRecordsEmitted(1000L).withBytesEmitted(1000L)
                  .withEstimatedBytes(1000L).withEstimatedRecords(1000L)
                  .withBytesCommitted(1000L).withRecordsCommitted(1000L),
              List.of(new StreamSyncStats().withStreamName("name1").withStats(
                  new SyncStats()
                      .withBytesEmitted(1000L).withRecordsEmitted(1000L)
                      .withEstimatedBytes(10000L).withEstimatedRecords(2000L))
                  .withWasBackfilled(false)
                  .withWasResumed(false),
                  new StreamSyncStats().withStreamName("name2").withStreamNamespace("ns")
                      .withStats(new SyncStats()
                          .withEstimatedBytes(100000L).withEstimatedRecords(20000L)
                          .withBytesEmitted(5000L).withRecordsEmitted(5000L))
                      .withWasBackfilled(true)
                      .withWasResumed(false))));

      assertEquals(Jsons.canonicalJsonSerialize(exp), Jsons.canonicalJsonSerialize(stats));

    }

    @Test
    @DisplayName("Writing stats for different streams should not have side effects")
    void testWritingStatsForDifferentStreams() throws IOException {
      final long jobOneId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
      final int jobOneAttemptNumberOne = jobPersistence.createAttempt(jobOneId, LOG_PATH);

      final String stream1 = "s1";
      final String namespace1 = "ns1";
      final String stream2 = "s2";
      final String namespace2 = "ns2";
      final String stream3 = "s3";
      final String namespace3 = null;

      final var streamStatsUpdate0 = List.of(
          new StreamSyncStats().withStreamName(stream1).withStreamNamespace(namespace1)
              .withStats(new SyncStats().withBytesEmitted(0L).withRecordsEmitted(0L)),
          new StreamSyncStats().withStreamName(stream2).withStreamNamespace(namespace2)
              .withStats(new SyncStats().withBytesEmitted(0L).withRecordsEmitted(0L)),
          new StreamSyncStats().withStreamName(stream3).withStreamNamespace(namespace3)
              .withStats(new SyncStats().withBytesEmitted(0L).withRecordsEmitted(0L)));
      jobPersistence.writeStats(jobOneId, jobOneAttemptNumberOne, null, null, null, null, 1000L, null, CONNECTION_ID, streamStatsUpdate0);

      final var streamStatsUpdate1 = List.of(
          new StreamSyncStats().withStreamName(stream1).withStreamNamespace(namespace1)
              .withStats(new SyncStats().withBytesEmitted(10L).withRecordsEmitted(1L)).withWasBackfilled(false).withWasResumed(false));
      jobPersistence.writeStats(jobOneId, jobOneAttemptNumberOne, null, null, 1L, 10L, 1000L, null, CONNECTION_ID, streamStatsUpdate1);

      final var streamStatsUpdate2 = List.of(
          new StreamSyncStats().withStreamName(stream2).withStreamNamespace(namespace2)
              .withStats(new SyncStats().withBytesEmitted(20L).withRecordsEmitted(2L)).withWasBackfilled(false).withWasResumed(false));
      jobPersistence.writeStats(jobOneId, jobOneAttemptNumberOne, null, null, 3L, 30L, 1000L, null, CONNECTION_ID, streamStatsUpdate2);

      final var streamStatsUpdate3 = List.of(
          new StreamSyncStats().withStreamName(stream3).withStreamNamespace(namespace3)
              .withStats(new SyncStats().withBytesEmitted(30L).withRecordsEmitted(3L)).withWasBackfilled(false).withWasResumed(false));
      jobPersistence.writeStats(jobOneId, jobOneAttemptNumberOne, null, null, 6L, 60L, 1000L, null, CONNECTION_ID, streamStatsUpdate3);

      final Map<JobAttemptPair, AttemptStats> stats = jobPersistence.getAttemptStats(List.of(jobOneId));
      final AttemptStats attempt1Stats = stats.get(new JobAttemptPair(jobOneId, jobOneAttemptNumberOne));

      final List<StreamSyncStats> actualStreamSyncStats1 = getStreamSyncStats(attempt1Stats, stream1, namespace1);
      assertEquals(streamStatsUpdate1, actualStreamSyncStats1);
      final List<StreamSyncStats> actualStreamSyncStats2 = getStreamSyncStats(attempt1Stats, stream2, namespace2);
      assertEquals(streamStatsUpdate2, actualStreamSyncStats2);
      final List<StreamSyncStats> actualStreamSyncStats3 = getStreamSyncStats(attempt1Stats, stream3, namespace3);
      assertEquals(streamStatsUpdate3, actualStreamSyncStats3);
    }

    private List<StreamSyncStats> getStreamSyncStats(final AttemptStats attemptStats, final String streamName, final String namespace) {
      return attemptStats.perStreamStats().stream()
          .filter(s -> s.getStreamName().equals(streamName) && (namespace == null || s.getStreamNamespace().equals(namespace)))
          .toList();
    }

    @Test
    @DisplayName("Retrieving stats for an empty list should not cause an exception.")
    void testGetStatsForEmptyJobList() throws IOException {
      assertNotNull(jobPersistence.getAttemptStats(List.of()));
    }

    @Test
    @DisplayName("Retrieving stats for a bad job attempt input should not cause an exception.")
    void testGetStatsForBadJobAttemptInput() throws IOException {
      assertNotNull(jobPersistence.getAttemptStats(-1, -1));
    }

    @Test
    @DisplayName("Combined stats can be retrieved without per stream stats.")
    void testGetAttemptCombinedStats() throws IOException {
      final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
      final int attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH);
      final var estimatedRecords = 1234L;
      final var estimatedBytes = 5678L;
      final var recordsEmitted = 9012L;
      final var bytesEmitted = 3456L;
      final var recordsCommitted = 7890L;
      final var bytesCommitted = 1234L;

      final var streamStats = List.of(
          new StreamSyncStats().withStreamName("name1").withStreamNamespace("ns")
              .withStats(new SyncStats().withBytesEmitted(500L).withRecordsEmitted(500L).withEstimatedBytes(10000L).withEstimatedRecords(2000L)));
      jobPersistence.writeStats(
          jobId, attemptNumber, estimatedRecords, estimatedBytes, recordsEmitted, bytesEmitted, recordsCommitted, bytesCommitted, CONNECTION_ID,
          streamStats);

      final SyncStats stats = jobPersistence.getAttemptCombinedStats(jobId, attemptNumber);
      assertEquals(estimatedRecords, stats.getEstimatedRecords());
      assertEquals(estimatedBytes, stats.getEstimatedBytes());
      assertEquals(recordsEmitted, stats.getRecordsEmitted());
      assertEquals(bytesEmitted, stats.getBytesEmitted());
      assertEquals(recordsCommitted, stats.getRecordsCommitted());
      assertEquals(bytesCommitted, stats.getBytesCommitted());
    }

  }

  @Nested
  class GetAndSetVersion {

    @Test
    void testSetVersion() throws IOException {
      final String version = UUID.randomUUID().toString();
      jobPersistence.setVersion(version);
      assertEquals(version, jobPersistence.getVersion().orElseThrow());
    }

    @Test
    void testSetVersionReplacesExistingId() throws IOException {
      final String deploymentId1 = UUID.randomUUID().toString();
      final String deploymentId2 = UUID.randomUUID().toString();
      jobPersistence.setVersion(deploymentId1);
      jobPersistence.setVersion(deploymentId2);
      assertEquals(deploymentId2, jobPersistence.getVersion().orElseThrow());
    }

  }

  @Nested
  class GetAndSetDeployment {

    @Test
    void testSetDeployment() throws IOException {
      final UUID deploymentId = UUID.randomUUID();
      jobPersistence.setDeployment(deploymentId);
      assertEquals(deploymentId, jobPersistence.getDeployment().orElseThrow());
    }

    @Test
    void testSetDeploymentIdDoesNotReplaceExistingId() throws IOException {
      final UUID deploymentId1 = UUID.randomUUID();
      final UUID deploymentId2 = UUID.randomUUID();
      jobPersistence.setDeployment(deploymentId1);
      jobPersistence.setDeployment(deploymentId2);
      assertEquals(deploymentId1, jobPersistence.getDeployment().orElseThrow());
    }

  }

  @Nested
  @DisplayName("When cancelling job")
  class CancelJob {

    @Test
    @DisplayName("Should cancel job and leave job in cancelled state")
    void testCancelJob() throws IOException {
      final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
      final Job created = jobPersistence.getJob(jobId);

      when(timeSupplier.get()).thenReturn(Instant.ofEpochMilli(4242));
      jobPersistence.cancelJob(jobId);

      final Job updated = jobPersistence.getJob(jobId);
      assertEquals(JobStatus.CANCELLED, updated.getStatus());
      assertNotEquals(created.getUpdatedAtInSecond(), updated.getUpdatedAtInSecond());
    }

    @Test
    @DisplayName("Should not raise an exception if job is already succeeded")
    void testCancelJobAlreadySuccessful() throws IOException {
      final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
      final int attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH);
      jobPersistence.succeedAttempt(jobId, attemptNumber);

      assertDoesNotThrow(() -> jobPersistence.cancelJob(jobId));

      final Job updated = jobPersistence.getJob(jobId);
      assertEquals(JobStatus.SUCCEEDED, updated.getStatus());
    }

  }

  @Nested
  @DisplayName("When creating attempt")
  class CreateAttempt {

    @Test
    @DisplayName("Should create an attempt")
    void testCreateAttempt() throws IOException {
      final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
      jobPersistence.createAttempt(jobId, LOG_PATH);

      final Job actual = jobPersistence.getJob(jobId);
      final Job expected = createJob(
          jobId,
          SPEC_JOB_CONFIG,
          JobStatus.RUNNING,
          Lists.newArrayList(createUnfinishedAttempt(0, jobId, AttemptStatus.RUNNING, LOG_PATH)),
          NOW.getEpochSecond());
      assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Should increment attempt id if creating multiple attempts")
    void testCreateAttemptAttemptId() throws IOException {
      final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
      final int attemptNumber1 = jobPersistence.createAttempt(jobId, LOG_PATH);
      jobPersistence.failAttempt(jobId, attemptNumber1);

      final Job jobAfterOneAttempts = jobPersistence.getJob(jobId);
      assertEquals(0, attemptNumber1);
      assertEquals(0, jobAfterOneAttempts.getAttempts().get(0).getAttemptNumber());

      final int attemptNumber2 = jobPersistence.createAttempt(jobId, LOG_PATH);
      final Job jobAfterTwoAttempts = jobPersistence.getJob(jobId);
      assertEquals(1, attemptNumber2);
      assertEquals(Sets.newHashSet(0, 1), jobAfterTwoAttempts.getAttempts().stream().map(Attempt::getAttemptNumber).collect(Collectors.toSet()));
    }

    @Test
    @DisplayName("Should not create an attempt if an attempt is running")
    void testCreateAttemptWhileAttemptAlreadyRunning() throws IOException {
      final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
      jobPersistence.createAttempt(jobId, LOG_PATH);

      assertThrows(IllegalStateException.class, () -> jobPersistence.createAttempt(jobId, LOG_PATH));

      final Job actual = jobPersistence.getJob(jobId);
      final Job expected = createJob(
          jobId,
          SPEC_JOB_CONFIG,
          JobStatus.RUNNING,
          Lists.newArrayList(createUnfinishedAttempt(0, jobId, AttemptStatus.RUNNING, LOG_PATH)),
          NOW.getEpochSecond());
      assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Should not create an attempt if job is in terminal state")
    void testCreateAttemptTerminal() throws IOException {
      final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
      final int attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH);
      jobPersistence.succeedAttempt(jobId, attemptNumber);

      assertThrows(IllegalStateException.class, () -> jobPersistence.createAttempt(jobId, LOG_PATH));

      final Job actual = jobPersistence.getJob(jobId);
      final Job expected = createJob(
          jobId,
          SPEC_JOB_CONFIG,
          JobStatus.SUCCEEDED,
          Lists.newArrayList(createAttempt(0, jobId, AttemptStatus.SUCCEEDED, LOG_PATH)),
          NOW.getEpochSecond());
      assertEquals(expected, actual);
    }

  }

  @Nested
  @DisplayName("Get an attempt")
  class GetAttempt {

    @Test
    @DisplayName("Should get an attempt by job id")
    void testGetAttemptSimple() throws IOException {
      final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
      final var num = jobPersistence.createAttempt(jobId, LOG_PATH);

      final Attempt actual = jobPersistence.getAttemptForJob(jobId, 0).get();
      final Attempt expected = createUnfinishedAttempt(num, jobId, AttemptStatus.RUNNING, LOG_PATH);

      assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Should get an attempt specified by attempt number")
    void testGetAttemptMultiple() throws IOException {
      final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();

      for (int i = 0; i < 10; ++i) {
        final int attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH);
        assertEquals(attemptNumber, i);

        final Attempt running = jobPersistence.getAttemptForJob(jobId, attemptNumber).get();
        final Attempt expectedRunning = createUnfinishedAttempt(attemptNumber, jobId, AttemptStatus.RUNNING, LOG_PATH);
        assertEquals(expectedRunning, running);

        jobPersistence.failAttempt(jobId, attemptNumber);

        final Attempt failed = jobPersistence.getAttemptForJob(jobId, attemptNumber).get();
        final Attempt expectedFailed = createAttempt(attemptNumber, jobId, AttemptStatus.FAILED, LOG_PATH);
        assertEquals(expectedFailed, failed);
      }

      final int last = jobPersistence.createAttempt(jobId, LOG_PATH);

      final Attempt running = jobPersistence.getAttemptForJob(jobId, last).get();
      final Attempt expectedRunning = createUnfinishedAttempt(last, jobId, AttemptStatus.RUNNING, LOG_PATH);
      assertEquals(expectedRunning, running);

      jobPersistence.succeedAttempt(jobId, last);

      final Attempt succeeded = jobPersistence.getAttemptForJob(jobId, last).get();
      final Attempt expectedFailed = createAttempt(last, jobId, AttemptStatus.SUCCEEDED, LOG_PATH);
      assertEquals(expectedFailed, succeeded);
    }

  }

  @Nested
  @DisplayName("List attempts after a given timestamp for a given connection")
  class ListAttemptsByConnectionByTimestamp {

    @Test
    @DisplayName("Returns only entries after the timestamp")
    void testListAttemptsForConnectionAfterTimestamp() throws IOException {

      final long jobId1 = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow();
      final int attemptId1 = jobPersistence.createAttempt(jobId1, LOG_PATH);
      jobPersistence.succeedAttempt(jobId1, attemptId1);

      final Instant addTwoSeconds = NOW.plusSeconds(2);
      when(timeSupplier.get()).thenReturn(addTwoSeconds);
      final Instant afterNow = NOW;

      final long jobId2 = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow();
      final int attemptId2 = jobPersistence.createAttempt(jobId2, LOG_PATH);
      jobPersistence.succeedAttempt(jobId2, attemptId2);

      final long jobId3 = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow();
      final int attemptId3 = jobPersistence.createAttempt(jobId3, LOG_PATH);
      jobPersistence.succeedAttempt(jobId3, attemptId3);

      final List<AttemptWithJobInfo> attempts = jobPersistence.listAttemptsForConnectionAfterTimestamp(CONNECTION_ID, ConfigType.SYNC,
          afterNow);

      assertEquals(2, attempts.size());
      assertEquals(jobId2, attempts.get(0).getJobInfo().getId());
      assertEquals(jobId3, attempts.get(1).getJobInfo().getId());
    }

  }

  @Nested
  @DisplayName("When enqueueing job")
  class EnqueueJob {

    @Test
    @DisplayName("Should create initial job without attempt")
    void testCreateJobAndGetWithoutAttemptJob() throws IOException {
      final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();

      final Job actual = jobPersistence.getJob(jobId);
      final Job expected = createJob(jobId, SPEC_JOB_CONFIG, JobStatus.PENDING, Collections.emptyList(), NOW.getEpochSecond());
      assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Should not create a second job if a job under the same scope is in a non-terminal state")
    void testCreateJobNoQueueing() throws IOException {
      final Optional<Long> jobId1 = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true);
      final Optional<Long> jobId2 = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true);

      assertTrue(jobId1.isPresent());
      assertTrue(jobId2.isEmpty());

      final Job actual = jobPersistence.getJob(jobId1.get());
      final Job expected = createJob(jobId1.get(), SYNC_JOB_CONFIG, JobStatus.PENDING, Collections.emptyList(), NOW.getEpochSecond());
      assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Should create a second job if a previous job under the same scope has failed")
    void testCreateJobIfPrevJobFailed() throws IOException {
      final Optional<Long> jobId1 = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true);
      assertTrue(jobId1.isPresent());

      jobPersistence.failJob(jobId1.get());
      final Optional<Long> jobId2 = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true);
      assertTrue(jobId2.isPresent());

      final Job actual = jobPersistence.getJob(jobId2.get());
      final Job expected = createJob(jobId2.get(), SYNC_JOB_CONFIG, JobStatus.PENDING, Collections.emptyList(), NOW.getEpochSecond());
      assertEquals(expected, actual);
    }

  }

  @Nested
  @DisplayName("When failing job")
  class FailJob {

    @Test
    @DisplayName("Should set job status to failed")
    void failJob() throws IOException {
      final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
      final Job created = jobPersistence.getJob(jobId);

      when(timeSupplier.get()).thenReturn(Instant.ofEpochMilli(4242));
      jobPersistence.failJob(jobId);

      final Job updated = jobPersistence.getJob(jobId);
      assertEquals(JobStatus.FAILED, updated.getStatus());
      assertNotEquals(created.getUpdatedAtInSecond(), updated.getUpdatedAtInSecond());
    }

    @Test
    @DisplayName("Should not raise an exception if job is already succeeded")
    void testFailJobAlreadySucceeded() throws IOException {
      final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
      final int attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH);
      jobPersistence.succeedAttempt(jobId, attemptNumber);

      assertDoesNotThrow(() -> jobPersistence.failJob(jobId));

      final Job updated = jobPersistence.getJob(jobId);
      assertEquals(JobStatus.SUCCEEDED, updated.getStatus());
    }

  }

  @Nested
  @DisplayName("When getting last replication job")
  class GetLastReplicationJob {

    @Test
    @DisplayName("Should return nothing if no job exists")
    void testGetLastReplicationJobForConnectionIdEmpty() throws IOException {
      final Optional<Job> actual = jobPersistence.getLastReplicationJob(CONNECTION_ID);

      assertTrue(actual.isEmpty());
    }

    @Test
    @DisplayName("Should return the last sync job")
    void testGetLastSyncJobForConnectionId() throws IOException {
      final long jobId1 = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow();
      jobPersistence.succeedAttempt(jobId1, jobPersistence.createAttempt(jobId1, LOG_PATH));

      final Instant afterNow = NOW.plusSeconds(1000);
      when(timeSupplier.get()).thenReturn(afterNow);
      final long jobId2 = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow();

      final Optional<Job> actual = jobPersistence.getLastReplicationJob(CONNECTION_ID);
      final Job expected = createJob(jobId2, SYNC_JOB_CONFIG, JobStatus.PENDING, Collections.emptyList(), afterNow.getEpochSecond());

      assertEquals(Optional.of(expected), actual);
    }

    @Test
    @DisplayName("Should return the last reset job")
    void testGetLastResetJobForConnectionId() throws IOException {
      final long jobId1 = jobPersistence.enqueueJob(SCOPE, RESET_JOB_CONFIG, true).orElseThrow();
      jobPersistence.succeedAttempt(jobId1, jobPersistence.createAttempt(jobId1, LOG_PATH));

      final Instant afterNow = NOW.plusSeconds(1000);
      when(timeSupplier.get()).thenReturn(afterNow);
      final long jobId2 = jobPersistence.enqueueJob(SCOPE, RESET_JOB_CONFIG, true).orElseThrow();

      final Optional<Job> actual = jobPersistence.getLastReplicationJob(CONNECTION_ID);
      final Job expected = createJob(jobId2, RESET_JOB_CONFIG, JobStatus.PENDING, Collections.emptyList(), afterNow.getEpochSecond());

      assertEquals(Optional.of(expected), actual);
    }

  }

  @Nested
  @DisplayName("When getting last sync job")
  class GetLastSyncJob {

    @Test
    @DisplayName("Should return nothing if no job exists")
    void testGetLastSyncJobForConnectionIdEmpty() throws IOException {
      final Optional<Job> actual = jobPersistence.getLastSyncJob(CONNECTION_ID);

      assertTrue(actual.isEmpty());
    }

    @Test
    @DisplayName("Should return the last enqueued sync job")
    void testGetLastSyncJobForConnectionId() throws IOException {
      final long jobId1 = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow();
      jobPersistence.succeedAttempt(jobId1, jobPersistence.createAttempt(jobId1, LOG_PATH));

      final Instant afterNow = NOW.plusSeconds(1000);
      when(timeSupplier.get()).thenReturn(afterNow);
      final long jobId2 = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow();
      final int attemptNumber = jobPersistence.createAttempt(jobId2, LOG_PATH);

      // Return the latest sync job even if failed
      jobPersistence.failAttempt(jobId2, attemptNumber);
      final Attempt attempt = jobPersistence.getJob(jobId2).getAttempts().stream().findFirst().orElseThrow();
      jobPersistence.failJob(jobId2);

      final Optional<Job> actual = jobPersistence.getLastSyncJob(CONNECTION_ID);
      final Job expected = createJob(jobId2, SYNC_JOB_CONFIG, JobStatus.FAILED, List.of(attempt), afterNow.getEpochSecond());

      assertEquals(Optional.of(expected), actual);
    }

    @Test
    @DisplayName("Should return nothing if only reset job exists")
    void testGetLastSyncJobForConnectionIdEmptyBecauseOnlyReset() throws IOException {
      final long jobId = jobPersistence.enqueueJob(SCOPE, RESET_JOB_CONFIG, true).orElseThrow();
      jobPersistence.succeedAttempt(jobId, jobPersistence.createAttempt(jobId, LOG_PATH));

      final Instant afterNow = NOW.plusSeconds(1000);
      when(timeSupplier.get()).thenReturn(afterNow);

      final Optional<Job> actual = jobPersistence.getLastSyncJob(CONNECTION_ID);

      assertTrue(actual.isEmpty());
    }

  }

  @Nested
  @DisplayName("When getting the last sync job for multiple connections")
  class GetLastSyncJobForConnections {

    private static final UUID CONNECTION_ID_1 = UUID.randomUUID();
    private static final UUID CONNECTION_ID_2 = UUID.randomUUID();
    private static final UUID CONNECTION_ID_3 = UUID.randomUUID();
    private static final String SCOPE_1 = CONNECTION_ID_1.toString();
    private static final String SCOPE_2 = CONNECTION_ID_2.toString();
    private static final String SCOPE_3 = CONNECTION_ID_3.toString();
    private static final List<UUID> CONNECTION_IDS = List.of(CONNECTION_ID_1, CONNECTION_ID_2, CONNECTION_ID_3);

    @Test
    @DisplayName("Should return nothing if no sync job exists")
    void testGetLastSyncJobsForConnectionsEmpty() throws IOException {
      final var actual = jobPersistence.getLastSyncJobForConnections(CONNECTION_IDS);

      assertTrue(actual.isEmpty());
    }

    @Test
    @DisplayName("Should return the last enqueued sync job for each connection")
    void testGetLastSyncJobForConnections() throws IOException {
      final long scope1Job1 = jobPersistence.enqueueJob(SCOPE_1, SYNC_JOB_CONFIG, true).orElseThrow();
      jobPersistence.succeedAttempt(scope1Job1, jobPersistence.createAttempt(scope1Job1, LOG_PATH));

      final long scope2Job1 = jobPersistence.enqueueJob(SCOPE_2, SYNC_JOB_CONFIG, true).orElseThrow();
      jobPersistence.succeedAttempt(scope2Job1, jobPersistence.createAttempt(scope2Job1, LOG_PATH));

      jobPersistence.enqueueJob(SCOPE_3, SYNC_JOB_CONFIG, true).orElseThrow();

      final Instant afterNow = NOW.plusSeconds(1000);
      when(timeSupplier.get()).thenReturn(afterNow);

      final long scope1Job2 = jobPersistence.enqueueJob(SCOPE_1, SYNC_JOB_CONFIG, true).orElseThrow();
      final int scope1Job2AttemptNumber = jobPersistence.createAttempt(scope1Job2, LOG_PATH);

      // should return the latest sync job even if failed
      jobPersistence.failAttempt(scope1Job2, scope1Job2AttemptNumber);
      jobPersistence.failJob(scope1Job2);

      // will leave this job running
      final long scope2Job2 = jobPersistence.enqueueJob(SCOPE_2, SYNC_JOB_CONFIG, true).orElseThrow();
      jobPersistence.createAttempt(scope2Job2, LOG_PATH);

      final List<JobStatusSummary> actual = jobPersistence.getLastSyncJobForConnections(CONNECTION_IDS);
      final List<JobStatusSummary> expected = new ArrayList<>();
      expected.add(new JobStatusSummary(CONNECTION_ID_1, afterNow.getEpochSecond(), JobStatus.FAILED));
      expected.add(new JobStatusSummary(CONNECTION_ID_2, afterNow.getEpochSecond(), JobStatus.RUNNING));
      expected.add(new JobStatusSummary(CONNECTION_ID_3, NOW.getEpochSecond(), JobStatus.PENDING));

      assertTrue(expected.size() == actual.size() && expected.containsAll(actual) && actual.containsAll(expected));
    }

    @Test
    @DisplayName("Should return nothing if only reset job exists")
    void testGetLastSyncJobsForConnectionsEmptyBecauseOnlyReset() throws IOException {
      final long jobId = jobPersistence.enqueueJob(SCOPE_1, RESET_JOB_CONFIG, true).orElseThrow();
      jobPersistence.succeedAttempt(jobId, jobPersistence.createAttempt(jobId, LOG_PATH));

      final Instant afterNow = NOW.plusSeconds(1000);
      when(timeSupplier.get()).thenReturn(afterNow);

      final var actual = jobPersistence.getLastSyncJobForConnections(CONNECTION_IDS);

      assertTrue(actual.isEmpty());
    }

  }

  @Nested
  @DisplayName("When getting the last running sync job for multiple connections")
  class GetRunningSyncJobForConnections {

    private static final UUID CONNECTION_ID_1 = UUID.randomUUID();
    private static final UUID CONNECTION_ID_2 = UUID.randomUUID();
    private static final UUID CONNECTION_ID_3 = UUID.randomUUID();
    private static final String SCOPE_1 = CONNECTION_ID_1.toString();
    private static final String SCOPE_2 = CONNECTION_ID_2.toString();
    private static final String SCOPE_3 = CONNECTION_ID_3.toString();
    private static final List<UUID> CONNECTION_IDS = List.of(CONNECTION_ID_1, CONNECTION_ID_2, CONNECTION_ID_3);

    @Test
    @DisplayName("Should return nothing if no sync job exists")
    void testGetRunningSyncJobsForConnectionsEmpty() throws IOException {
      final List<Job> actual = jobPersistence.getRunningSyncJobForConnections(CONNECTION_IDS);

      assertTrue(actual.isEmpty());
    }

    @Test
    @DisplayName("Should return the last running sync job for each connection")
    void testGetRunningSyncJobsForConnections() throws IOException {
      // succeeded jobs should not be present in the result
      final long scope1Job1 = jobPersistence.enqueueJob(SCOPE_1, SYNC_JOB_CONFIG, true).orElseThrow();
      jobPersistence.succeedAttempt(scope1Job1, jobPersistence.createAttempt(scope1Job1, LOG_PATH));

      // fail scope2's first job, but later start a running job that should show up in the result
      final long scope2Job1 = jobPersistence.enqueueJob(SCOPE_2, SYNC_JOB_CONFIG, true).orElseThrow();
      final int scope2Job1AttemptNumber = jobPersistence.createAttempt(scope2Job1, LOG_PATH);
      jobPersistence.failAttempt(scope2Job1, scope2Job1AttemptNumber);
      jobPersistence.failJob(scope2Job1);

      // pending jobs should be present in the result
      final long scope3Job1 = jobPersistence.enqueueJob(SCOPE_3, SYNC_JOB_CONFIG, true).orElseThrow();

      final Instant afterNow = NOW.plusSeconds(1000);
      when(timeSupplier.get()).thenReturn(afterNow);

      // create a running job/attempt for scope2
      final long scope2Job2 = jobPersistence.enqueueJob(SCOPE_2, SYNC_JOB_CONFIG, true).orElseThrow();
      jobPersistence.createAttempt(scope2Job2, LOG_PATH);
      final Attempt scope2Job2attempt = jobPersistence.getJob(scope2Job2).getAttempts().stream().findFirst().orElseThrow();

      final List<Job> expected = new ArrayList<>();
      expected.add(createJob(scope2Job2, SYNC_JOB_CONFIG, JobStatus.RUNNING, List.of(scope2Job2attempt), afterNow.getEpochSecond(), SCOPE_2, true));
      expected.add(createJob(scope3Job1, SYNC_JOB_CONFIG, JobStatus.PENDING, Collections.emptyList(), NOW.getEpochSecond(), SCOPE_3, true));

      final List<Job> actual = jobPersistence.getRunningSyncJobForConnections(CONNECTION_IDS);
      assertTrue(expected.size() == actual.size() && expected.containsAll(actual) && actual.containsAll(expected));
    }

    @Test
    @DisplayName("Should return nothing if only a running reset job exists")
    void testGetRunningSyncJobsForConnectionsEmptyBecauseOnlyReset() throws IOException {
      final long jobId = jobPersistence.enqueueJob(SCOPE_1, RESET_JOB_CONFIG, true).orElseThrow();
      jobPersistence.createAttempt(jobId, LOG_PATH);

      final Instant afterNow = NOW.plusSeconds(1000);
      when(timeSupplier.get()).thenReturn(afterNow);

      final List<Job> actual = jobPersistence.getRunningSyncJobForConnections(CONNECTION_IDS);

      assertTrue(actual.isEmpty());
    }

  }

  @Nested
  @DisplayName("When getting a running job for a single")
  class GetRunningJobForConnection {

    private static final UUID CONNECTION_ID_1 = UUID.randomUUID();

    private static final String SCOPE_1 = CONNECTION_ID_1.toString();

    @Test
    @DisplayName("Should return nothing if no sync job exists")
    void testGetRunningSyncJobsForConnectionsEmpty() throws IOException {
      final List<Job> actual = jobPersistence.getRunningJobForConnection(CONNECTION_ID_1);

      assertTrue(actual.isEmpty());
    }

    @Test
    @DisplayName("Should return a running sync job for the connection")
    void testGetRunningJobForConnection() throws IOException {
      final long scope1Job1 = jobPersistence.enqueueJob(SCOPE_1, SYNC_JOB_CONFIG, true).orElseThrow();
      jobPersistence.createAttempt(scope1Job1, LOG_PATH);
      final Attempt scope1Job1Attempt = jobPersistence.getJob(scope1Job1).getAttempts().stream().findFirst().orElseThrow();

      final Instant afterNow = NOW;
      when(timeSupplier.get()).thenReturn(afterNow);

      final List<Job> expected = new ArrayList<>();
      expected.add(createJob(scope1Job1, SYNC_JOB_CONFIG, JobStatus.RUNNING, List.of(scope1Job1Attempt), afterNow.getEpochSecond(), SCOPE_1, true));

      final List<Job> actual = jobPersistence.getRunningJobForConnection(CONNECTION_ID_1);
      assertTrue(expected.size() == actual.size() && expected.containsAll(actual) && actual.containsAll(expected));
    }

    @Test
    @DisplayName("Should return job if only a running reset job exists")
    void testGetRunningSyncJobsForConnectionsEmptyBecauseOnlyReset() throws IOException {
      final long jobId = jobPersistence.enqueueJob(SCOPE_1, RESET_JOB_CONFIG, true).orElseThrow();
      jobPersistence.createAttempt(jobId, LOG_PATH);
      final Attempt scope1Job1Attempt = jobPersistence.getJob(jobId).getAttempts().stream().findFirst().orElseThrow();

      final Instant afterNow = NOW;
      when(timeSupplier.get()).thenReturn(afterNow);

      final List<Job> expected = new ArrayList<>();
      expected.add(createJob(jobId, RESET_JOB_CONFIG, JobStatus.RUNNING, List.of(scope1Job1Attempt), afterNow.getEpochSecond(), SCOPE_1, true));

      final List<Job> actual = jobPersistence.getRunningJobForConnection(CONNECTION_ID_1);

      assertTrue(expected.size() == actual.size() && expected.containsAll(actual) && actual.containsAll(expected));
    }

  }

  @Nested
  @DisplayName("When getting first replication job")
  class GetFirstReplicationJob {

    @Test
    @DisplayName("Should return nothing if no job exists")
    void testGetFirstSyncJobForConnectionIdEmpty() throws IOException {
      final Optional<Job> actual = jobPersistence.getFirstReplicationJob(CONNECTION_ID);

      assertTrue(actual.isEmpty());
    }

    @Test
    @DisplayName("Should return the first job")
    void testGetFirstSyncJobForConnectionId() throws IOException {
      final long jobId1 = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow();
      jobPersistence.succeedAttempt(jobId1, jobPersistence.createAttempt(jobId1, LOG_PATH));
      final List<AttemptWithJobInfo> attemptsWithJobInfo =
          jobPersistence.listAttemptsWithJobInfo(SYNC_JOB_CONFIG.getConfigType(), Instant.EPOCH, 1000);
      final List<Attempt> attempts = Collections.singletonList(attemptsWithJobInfo.get(0).getAttempt());

      final Instant afterNow = NOW.plusSeconds(1000);
      when(timeSupplier.get()).thenReturn(afterNow);
      jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow();

      final Optional<Job> actual = jobPersistence.getFirstReplicationJob(CONNECTION_ID);
      final Job expected = createJob(jobId1, SYNC_JOB_CONFIG, JobStatus.SUCCEEDED, attempts, NOW.getEpochSecond());

      assertEquals(Optional.of(expected), actual);
    }

  }

  @Nested
  @DisplayName("When getting the count of jobs")
  class GetJobCount {

    @Test
    @DisplayName("Should return the total job count for all connections in any status")
    void testGetJobCount() throws IOException {
      final int numJobsToCreate = 10;
      final List<Long> ids = new ArrayList<>();
      // create jobs for connection 1
      for (int i = 0; i < numJobsToCreate / 2; i++) {
        final Long jobId = jobPersistence.enqueueJob(CONNECTION_ID.toString(), SPEC_JOB_CONFIG, true).orElseThrow();
        ids.add(jobId);
      }

      // create jobs for connection 2
      for (int i = 0; i < numJobsToCreate / 2; i++) {
        final Long jobId = jobPersistence.enqueueJob(CONNECTION_ID2.toString(), SPEC_JOB_CONFIG, true).orElseThrow();
        ids.add(jobId);
      }

      // fail some jobs
      for (int i = 0; i < 3; i++) {
        jobPersistence.failJob(ids.get(i));
      }

      final Long actualJobCount =
          jobPersistence.getJobCount(Set.of(SPEC_JOB_CONFIG.getConfigType()), null, null, null, null, null, null);

      assertEquals(numJobsToCreate, actualJobCount);
    }

    @Test
    @DisplayName("Should return the total job count for the connection")
    void testGetJobCountWithConnectionFilter() throws IOException {
      final int numJobsToCreate = 10;
      for (int i = 0; i < numJobsToCreate; i++) {
        jobPersistence.enqueueJob(CONNECTION_ID.toString(), SPEC_JOB_CONFIG, true);
      }

      final Long actualJobCount =
          jobPersistence.getJobCount(Set.of(SPEC_JOB_CONFIG.getConfigType()), CONNECTION_ID.toString(), null, null, null, null, null);

      assertEquals(numJobsToCreate, actualJobCount);
    }

    @Test
    @DisplayName("Should return the total job count for the connection when filtering by failed jobs only")
    void testGetJobCountWithFailedJobFilter() throws IOException {
      final int numPendingJobsToCreate = 10;
      for (int i = 0; i < numPendingJobsToCreate; i++) {
        jobPersistence.enqueueJob(SCOPE, CHECK_JOB_CONFIG, true);
      }

      final int numFailedJobsToCreate = 5;
      for (int i = 0; i < numFailedJobsToCreate; i++) {
        final Long jobId = jobPersistence.enqueueJob(SCOPE, CHECK_JOB_CONFIG, true).orElseThrow();
        jobPersistence.failJob(jobId);
      }

      final Long actualJobCount =
          jobPersistence.getJobCount(Set.of(CHECK_JOB_CONFIG.getConfigType()), SCOPE, List.of(JobStatus.FAILED), null, null, null, null);

      assertEquals(numFailedJobsToCreate, actualJobCount);
    }

    @Test
    @DisplayName("Should return the total job count for the connection when filtering by failed and cancelled jobs only")
    void testGetJobCountWithFailedAndCancelledJobFilter() throws IOException {
      final Long jobId1 = jobPersistence.enqueueJob(SCOPE, CHECK_JOB_CONFIG, true).orElseThrow();
      jobPersistence.failJob(jobId1);

      final Long jobId2 = jobPersistence.enqueueJob(SCOPE, CHECK_JOB_CONFIG, true).orElseThrow();
      jobPersistence.cancelJob(jobId2);

      jobPersistence.enqueueJob(SCOPE, CHECK_JOB_CONFIG, true).orElseThrow();

      final Long actualJobCount =
          jobPersistence.getJobCount(Set.of(CHECK_JOB_CONFIG.getConfigType()), SCOPE, List.of(JobStatus.FAILED, JobStatus.CANCELLED), null, null,
              null, null);

      assertEquals(2, actualJobCount);
    }

    @Test
    @DisplayName("Should return the total job count filtering by createdAtStart")
    void testGetJobCountWithCreatedAtStart() throws IOException {
      final Long jobId = jobPersistence.enqueueJob(SCOPE, CHECK_JOB_CONFIG, true).orElseThrow();
      final Job job = jobPersistence.getJob(jobId);
      final Long jobCreatedAtSeconds = job.getCreatedAtInSecond();

      final OffsetDateTime oneHourEarlier = OffsetDateTime.ofInstant(Instant.ofEpochSecond((jobCreatedAtSeconds)), ZoneOffset.UTC).minusHours(1);
      final OffsetDateTime oneHourLater = OffsetDateTime.ofInstant(Instant.ofEpochSecond((jobCreatedAtSeconds)), ZoneOffset.UTC).plusHours(1);

      final Long numJobsCreatedAtStartOneHourEarlier =
          jobPersistence.getJobCount(Set.of(CHECK_JOB_CONFIG.getConfigType()), SCOPE, null, oneHourEarlier, null, null, null);
      final Long numJobsCreatedAtStartOneHourLater =
          jobPersistence.getJobCount(Set.of(CHECK_JOB_CONFIG.getConfigType()), SCOPE, null, oneHourLater, null, null, null);

      assertEquals(1, numJobsCreatedAtStartOneHourEarlier);
      assertEquals(0, numJobsCreatedAtStartOneHourLater);
    }

    @Test
    @DisplayName("Should return the total job count filtering by createdAtEnd")
    void testGetJobCountCreatedAtEnd() throws IOException {
      final Long jobId = jobPersistence.enqueueJob(SCOPE, CHECK_JOB_CONFIG, true).orElseThrow();
      final Job job = jobPersistence.getJob(jobId);
      final Long jobCreatedAtSeconds = job.getCreatedAtInSecond();

      final OffsetDateTime oneHourEarlier = OffsetDateTime.ofInstant(Instant.ofEpochSecond(jobCreatedAtSeconds), ZoneOffset.UTC).minusHours(1);
      final OffsetDateTime oneHourLater = OffsetDateTime.ofInstant(Instant.ofEpochSecond(jobCreatedAtSeconds), ZoneOffset.UTC).plusHours(1);

      final Long numJobsCreatedAtEndOneHourEarlier =
          jobPersistence.getJobCount(Set.of(CHECK_JOB_CONFIG.getConfigType()), SCOPE, null, null, oneHourEarlier, null, null);
      final Long numJobsCreatedAtEndOneHourLater =
          jobPersistence.getJobCount(Set.of(CHECK_JOB_CONFIG.getConfigType()), SCOPE, null, null, oneHourLater, null, null);

      assertEquals(0, numJobsCreatedAtEndOneHourEarlier);
      assertEquals(1, numJobsCreatedAtEndOneHourLater);
    }

    @Test
    @DisplayName("Should return the total job count filtering by updatedAtStart")
    void testGetJobCountWithUpdatedAtStart() throws IOException {
      final Long jobId = jobPersistence.enqueueJob(SCOPE, CHECK_JOB_CONFIG, true).orElseThrow();
      final Job job = jobPersistence.getJob(jobId);
      final Long jobUpdatedAtSeconds = job.getUpdatedAtInSecond();

      final OffsetDateTime oneHourEarlier = OffsetDateTime.ofInstant(Instant.ofEpochSecond((jobUpdatedAtSeconds)), ZoneOffset.UTC).minusHours(1);
      final OffsetDateTime oneHourLater = OffsetDateTime.ofInstant(Instant.ofEpochSecond((jobUpdatedAtSeconds)), ZoneOffset.UTC).plusHours(1);

      final Long numJobsUpdatedAtStartOneHourEarlier =
          jobPersistence.getJobCount(Set.of(CHECK_JOB_CONFIG.getConfigType()), SCOPE, null, null, null, oneHourEarlier, null);
      final Long numJobsUpdatedAtStartOneDayLater =
          jobPersistence.getJobCount(Set.of(CHECK_JOB_CONFIG.getConfigType()), SCOPE, null, null, null, oneHourLater, null);

      assertEquals(1, numJobsUpdatedAtStartOneHourEarlier);
      assertEquals(0, numJobsUpdatedAtStartOneDayLater);
    }

    @Test
    @DisplayName("Should return the total job count filtering by updatedAtEnd")
    void testGetJobCountUpdatedAtEnd() throws IOException {
      final Long jobId = jobPersistence.enqueueJob(SCOPE, CHECK_JOB_CONFIG, true).orElseThrow();
      final Job job = jobPersistence.getJob(jobId);
      final Long jobUpdatedAtSeconds = job.getUpdatedAtInSecond();

      final OffsetDateTime oneHourEarlier = OffsetDateTime.ofInstant(Instant.ofEpochSecond((jobUpdatedAtSeconds)), ZoneOffset.UTC).minusHours(1);
      final OffsetDateTime oneHourLater = OffsetDateTime.ofInstant(Instant.ofEpochSecond((jobUpdatedAtSeconds)), ZoneOffset.UTC).plusHours(1);

      final Long numJobsUpdatedAtEndOneHourEarlier =
          jobPersistence.getJobCount(Set.of(CHECK_JOB_CONFIG.getConfigType()), SCOPE, null, null, null, null, oneHourEarlier);
      final Long numJobsUpdatedAtEndOneHourLater =
          jobPersistence.getJobCount(Set.of(CHECK_JOB_CONFIG.getConfigType()), SCOPE, null, null, null, null, oneHourLater);

      assertEquals(0, numJobsUpdatedAtEndOneHourEarlier);
      assertEquals(1, numJobsUpdatedAtEndOneHourLater);
    }

    @Test
    @DisplayName("Should return 0 if there are no jobs for this connection")
    void testGetJobCountNoneForConnection() throws IOException {
      final UUID otherConnectionId1 = UUID.randomUUID();
      final UUID otherConnectionId2 = UUID.randomUUID();

      jobPersistence.enqueueJob(otherConnectionId1.toString(), SPEC_JOB_CONFIG, true);
      jobPersistence.enqueueJob(otherConnectionId2.toString(), SPEC_JOB_CONFIG, true);

      final Long actualJobCount =
          jobPersistence.getJobCount(Set.of(SPEC_JOB_CONFIG.getConfigType()), CONNECTION_ID.toString(), null, null, null, null, null);

      assertEquals(0, actualJobCount);
    }

  }

  @Nested
  @DisplayName("When listing jobs, use paged results")
  class ListJobs {

    @Test
    @DisplayName("Should return the correct page of results with multiple pages of history")
    void testListJobsByPage() throws IOException {
      final List<Long> ids = new ArrayList<>();
      for (int i = 0; i < 50; i++) {
        final long jobId = jobPersistence.enqueueJob(CONNECTION_ID.toString(), SPEC_JOB_CONFIG, true).orElseThrow();
        ids.add(jobId);

        // create two attempts per job to verify pagination is applied at the job record level
        final int attemptNum1 = jobPersistence.createAttempt(jobId, LOG_PATH);
        jobPersistence.failAttempt(jobId, attemptNum1);
        jobPersistence.createAttempt(jobId, LOG_PATH);

        // also create a job for another connection, to verify the query is properly filtering down to only
        // jobs for the desired connection
        jobPersistence.enqueueJob(CONNECTION_ID2.toString(), SPEC_JOB_CONFIG, true).orElseThrow();
      }

      final int pagesize = 10;
      final List<Job> actualList = jobPersistence.listJobs(Set.of(SPEC_JOB_CONFIG.getConfigType()), CONNECTION_ID.toString(), pagesize);
      assertEquals(pagesize, actualList.size());
      assertEquals(ids.get(ids.size() - 1), actualList.get(0).getId());
    }

    @Test
    @DisplayName("Should return the results in the correct sort order")
    void testListJobsSortsDescending() throws IOException {
      final List<Long> ids = new ArrayList<Long>();
      for (int i = 0; i < 100; i++) {
        // These have strictly the same created_at due to the setup() above, so should come back sorted by
        // id desc instead.
        final long jobId = jobPersistence.enqueueJob(CONNECTION_ID.toString(), SPEC_JOB_CONFIG, true).orElseThrow();
        ids.add(jobId);
      }
      final int pagesize = 200;
      final List<Job> actualList = jobPersistence.listJobs(Set.of(SPEC_JOB_CONFIG.getConfigType()), CONNECTION_ID.toString(), pagesize);
      for (int i = 0; i < 100; i++) {
        assertEquals(ids.get(ids.size() - (i + 1)), actualList.get(i).getId(), "Job ids should have been in order but weren't.");
      }
    }

    @Test
    @DisplayName("Should list all jobs")
    void testListJobs() throws IOException {
      final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();

      final List<Job> actualList = jobPersistence.listJobs(Set.of(SPEC_JOB_CONFIG.getConfigType()), CONNECTION_ID.toString(), 9999);

      final Job actual = actualList.get(0);
      final Job expected = createJob(jobId, SPEC_JOB_CONFIG, JobStatus.PENDING, Collections.emptyList(), NOW.getEpochSecond());

      assertEquals(1, actualList.size());
      assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Should list all jobs matching multiple config types")
    void testListJobsMultipleConfigTypes() throws IOException {
      final long specJobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
      final long checkJobId = jobPersistence.enqueueJob(SCOPE, CHECK_JOB_CONFIG, true).orElseThrow();
      // add a third config type that is not added in the listJobs request, to verify that it is not
      // included in the results
      jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow();

      final List<Job> actualList =
          jobPersistence.listJobs(Set.of(SPEC_JOB_CONFIG.getConfigType(), CHECK_JOB_CONFIG.getConfigType()), CONNECTION_ID.toString(), 9999);

      final List<Job> expectedList =
          List.of(createJob(checkJobId, CHECK_JOB_CONFIG, JobStatus.PENDING, Collections.emptyList(), NOW.getEpochSecond()),
              createJob(specJobId, SPEC_JOB_CONFIG, JobStatus.PENDING, Collections.emptyList(), NOW.getEpochSecond()));

      assertEquals(expectedList, actualList);
    }

    @Test
    @DisplayName("Should list all jobs with all attempts")
    void testListJobsWithMultipleAttempts() throws IOException {
      final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
      final int attemptNumber0 = jobPersistence.createAttempt(jobId, LOG_PATH);

      jobPersistence.failAttempt(jobId, attemptNumber0);

      final Path secondAttemptLogPath = LOG_PATH.resolve("2");
      final int attemptNumber1 = jobPersistence.createAttempt(jobId, secondAttemptLogPath);

      jobPersistence.succeedAttempt(jobId, attemptNumber1);

      final List<Job> actualList = jobPersistence.listJobs(Set.of(SPEC_JOB_CONFIG.getConfigType()), CONNECTION_ID.toString(), 9999);

      final Job actual = actualList.get(0);
      final Job expected = createJob(
          jobId,
          SPEC_JOB_CONFIG,
          JobStatus.SUCCEEDED,
          Lists.newArrayList(
              createAttempt(0, jobId, AttemptStatus.FAILED, LOG_PATH),
              createAttempt(1, jobId, AttemptStatus.SUCCEEDED, secondAttemptLogPath)),
          NOW.getEpochSecond());

      assertEquals(1, actualList.size());
      assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Should list all jobs with all attempts in descending order")
    void testListJobsWithMultipleAttemptsInDescOrder() throws IOException {
      // create first job with multiple attempts
      final var jobId1 = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
      final var job1Attempt1 = jobPersistence.createAttempt(jobId1, LOG_PATH);
      jobPersistence.failAttempt(jobId1, job1Attempt1);
      final var job1Attempt2LogPath = LOG_PATH.resolve("2");
      final int job1Attempt2 = jobPersistence.createAttempt(jobId1, job1Attempt2LogPath);
      jobPersistence.succeedAttempt(jobId1, job1Attempt2);

      // create second job with multiple attempts
      final var laterTime = NOW.plusSeconds(1000);
      when(timeSupplier.get()).thenReturn(laterTime);
      final var jobId2 = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
      final var job2Attempt1LogPath = LOG_PATH.resolve("3");
      final var job2Attempt1 = jobPersistence.createAttempt(jobId2, job2Attempt1LogPath);
      jobPersistence.succeedAttempt(jobId2, job2Attempt1);

      final List<Job> actualList = jobPersistence.listJobs(Set.of(SPEC_JOB_CONFIG.getConfigType()), CONNECTION_ID.toString(), 9999);

      assertEquals(2, actualList.size());
      assertEquals(jobId2, actualList.get(0).getId());
    }

    @Test
    @DisplayName("Should apply limits after ordering by the key provided by the caller")
    void testListJobsOrderedByUpdatedAt() throws IOException {

      final var jobId1 = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
      final var job1Attempt1 = jobPersistence.createAttempt(jobId1, LOG_PATH);

      final var laterTime = NOW.plusSeconds(1000);
      when(timeSupplier.get()).thenReturn(laterTime);
      final var jobId2 = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
      final var job2Attempt1LogPath = LOG_PATH.resolve("3");
      final var job2Attempt1 = jobPersistence.createAttempt(jobId2, job2Attempt1LogPath);
      jobPersistence.succeedAttempt(jobId2, job2Attempt1);

      final var evenLaterTime = NOW.plusSeconds(3000);
      when(timeSupplier.get()).thenReturn(evenLaterTime);
      jobPersistence.succeedAttempt(jobId1, job1Attempt1);

      String configId = null;
      final List<Job> updatedAtJobs =
          jobPersistence.listJobs(Set.of(SPEC_JOB_CONFIG.getConfigType()), configId, 1, 0, null, null, null, null, null, "updatedAt", "ASC");
      assertEquals(1, updatedAtJobs.size());
      assertEquals(jobId2, updatedAtJobs.get(0).getId());
      final List<Job> createdAtJobs =
          jobPersistence.listJobs(Set.of(SPEC_JOB_CONFIG.getConfigType()), configId, 1, 0, null, null, null, null, null, "createdAt", "ASC");
      assertEquals(1, createdAtJobs.size());
      assertEquals(jobId1, createdAtJobs.get(0).getId());
    }

    @Test
    @DisplayName("Should list jobs across all connections in any status")
    void testListJobsWithNoFilters() throws IOException {
      final int numJobsToCreate = 10;
      final List<Long> ids = new ArrayList<>();
      for (int i = 0; i < numJobsToCreate / 2; i++) {
        final Long connection1JobId = jobPersistence.enqueueJob(CONNECTION_ID.toString(), SPEC_JOB_CONFIG, true).orElseThrow();
        ids.add(connection1JobId);
      }

      for (int i = 0; i < numJobsToCreate / 2; i++) {
        final Long connection2JobId = jobPersistence.enqueueJob(CONNECTION_ID2.toString(), SPEC_JOB_CONFIG, true).orElseThrow();
        ids.add(connection2JobId);
      }

      // fail some jobs
      for (int i = 0; i < 3; i++) {
        jobPersistence.failJob(ids.get(i));
      }

      final String connectionId = null;
      final List<Job> jobs = jobPersistence.listJobs(
          Set.of(SPEC_JOB_CONFIG.getConfigType()),
          connectionId,
          9999,
          0,
          null,
          null,
          null,
          null,
          null,
          null,
          null);

      assertEquals(new HashSet<>(ids), jobs.stream().map(Job::getId).collect(Collectors.toSet()));
    }

    @Test
    @DisplayName("Should list jobs for one connection only")
    void testListJobsWithConnectionFilters() throws IOException {
      final int numJobsToCreate = 10;
      final Set<Long> idsConnection1 = new HashSet<>();
      for (int i = 0; i < numJobsToCreate / 2; i++) {
        final Long connection1JobId = jobPersistence.enqueueJob(CONNECTION_ID.toString(), SPEC_JOB_CONFIG, true).orElseThrow();
        idsConnection1.add(connection1JobId);
      }

      for (int i = 0; i < numJobsToCreate / 2; i++) {
        jobPersistence.enqueueJob(CONNECTION_ID2.toString(), SPEC_JOB_CONFIG, true).orElseThrow();
      }

      final List<Job> jobs = jobPersistence.listJobs(
          Set.of(SPEC_JOB_CONFIG.getConfigType()),
          CONNECTION_ID.toString(),
          9999,
          0,
          null,
          null,
          null,
          null,
          null,
          null,
          null);

      assertEquals(idsConnection1, jobs.stream().map(Job::getId).collect(Collectors.toSet()));
    }

    @Test
    @DisplayName("Should list jobs filtering by failed and cancelled jobs")
    void testListJobWithFailedAndCancelledJobFilter() throws IOException {
      final Long jobId1 = jobPersistence.enqueueJob(CONNECTION_ID.toString(), SPEC_JOB_CONFIG, true).orElseThrow();
      jobPersistence.failJob(jobId1);

      final Long jobId2 = jobPersistence.enqueueJob(CONNECTION_ID.toString(), SPEC_JOB_CONFIG, true).orElseThrow();
      jobPersistence.cancelJob(jobId2);

      final Long jobId3 = jobPersistence.enqueueJob(CONNECTION_ID.toString(), SPEC_JOB_CONFIG, true).orElseThrow();

      final List<Job> jobs = jobPersistence.listJobs(
          Set.of(SPEC_JOB_CONFIG.getConfigType()),
          CONNECTION_ID.toString(),
          9999,
          0,
          List.of(JobStatus.FAILED, JobStatus.CANCELLED),
          null,
          null,
          null,
          null,
          null,
          null);

      final Set<Long> actualIds = jobs.stream().map(Job::getId).collect(Collectors.toSet());
      assertEquals(2, actualIds.size());
      assertFalse(actualIds.contains(jobId3));
      assertTrue(actualIds.contains(jobId1));
      assertTrue(actualIds.contains(jobId2));
    }

    @Test
    @DisplayName("Should list jobs including the specified job across all connections")
    void testListJobsIncludingId() throws IOException {
      final List<Long> ids = new ArrayList<>();
      for (int i = 0; i < 100; i++) {
        // This makes each enqueued job have an increasingly higher createdAt time
        when(timeSupplier.get()).thenReturn(Instant.ofEpochSecond(i));
        // Alternate between spec and check job config types to verify that both config types are fetched
        // properly
        final JobConfig jobConfig = i % 2 == 0 ? SPEC_JOB_CONFIG : CHECK_JOB_CONFIG;
        // spread across different connections
        final String connectionId = i % 4 == 0 ? CONNECTION_ID.toString() : CONNECTION_ID2.toString();
        final long jobId = jobPersistence.enqueueJob(connectionId, jobConfig, true).orElseThrow();
        ids.add(jobId);
        // also create an attempt for each job to verify that joining with attempts does not cause failures
        jobPersistence.createAttempt(jobId, LOG_PATH);
      }

      final int includingIdIndex = 90;
      final int pageSize = 25;
      final List<Job> actualList = jobPersistence.listJobsIncludingId(Set.of(SPEC_JOB_CONFIG.getConfigType(), CHECK_JOB_CONFIG.getConfigType()),
          null, ids.get(includingIdIndex), pageSize);
      final List<Long> expectedJobIds = Lists.reverse(ids.subList(ids.size() - pageSize, ids.size()));
      assertEquals(expectedJobIds, actualList.stream().map(Job::getId).toList());
    }

    @Test
    @DisplayName("Should list jobs including the specified job")
    void testListJobsIncludingIdWithConnectionFilter() throws IOException {
      final List<Long> ids = new ArrayList<>();
      for (int i = 0; i < 100; i++) {
        // This makes each enqueued job have an increasingly higher createdAt time
        when(timeSupplier.get()).thenReturn(Instant.ofEpochSecond(i));
        // Alternate between spec and check job config types to verify that both config types are fetched
        // properly
        final JobConfig jobConfig = i % 2 == 0 ? SPEC_JOB_CONFIG : CHECK_JOB_CONFIG;
        final long jobId = jobPersistence.enqueueJob(CONNECTION_ID.toString(), jobConfig, true).orElseThrow();
        ids.add(jobId);
        // also create an attempt for each job to verify that joining with attempts does not cause failures
        jobPersistence.createAttempt(jobId, LOG_PATH);
      }

      final int includingIdIndex = 90;
      final int pageSize = 25;
      final List<Job> actualList = jobPersistence.listJobsIncludingId(Set.of(SPEC_JOB_CONFIG.getConfigType(), CHECK_JOB_CONFIG.getConfigType()),
          CONNECTION_ID.toString(), ids.get(includingIdIndex), pageSize);
      final List<Long> expectedJobIds = Lists.reverse(ids.subList(ids.size() - pageSize, ids.size()));
      assertEquals(expectedJobIds, actualList.stream().map(Job::getId).toList());
    }

    @Test
    @DisplayName("Should list jobs including the specified job, including multiple pages if necessary")
    void testListJobsIncludingIdMultiplePages() throws IOException {
      final List<Long> ids = new ArrayList<>();
      for (int i = 0; i < 100; i++) {
        // This makes each enqueued job have an increasingly higher createdAt time
        when(timeSupplier.get()).thenReturn(Instant.ofEpochSecond(i));
        // Alternate between spec and check job config types to verify that both config types are fetched
        // properly
        final JobConfig jobConfig = i % 2 == 0 ? SPEC_JOB_CONFIG : CHECK_JOB_CONFIG;
        final long jobId = jobPersistence.enqueueJob(CONNECTION_ID.toString(), jobConfig, true).orElseThrow();
        ids.add(jobId);
        // also create an attempt for each job to verify that joining with attempts does not cause failures
        jobPersistence.createAttempt(jobId, LOG_PATH);
      }

      // including id is on the second page, so response should contain two pages of jobs
      final int includingIdIndex = 60;
      final int pageSize = 25;
      final List<Job> actualList = jobPersistence.listJobsIncludingId(Set.of(SPEC_JOB_CONFIG.getConfigType(), CHECK_JOB_CONFIG.getConfigType()),
          CONNECTION_ID.toString(), ids.get(includingIdIndex), pageSize);
      final List<Long> expectedJobIds = Lists.reverse(ids.subList(ids.size() - (pageSize * 2), ids.size()));
      assertEquals(expectedJobIds, actualList.stream().map(Job::getId).toList());
    }

    @Test
    @DisplayName("Should return an empty list if there is no job with the includingJob ID for this connection")
    void testListJobsIncludingIdFromWrongConnection() throws IOException {
      for (int i = 0; i < 10; i++) {
        jobPersistence.enqueueJob(CONNECTION_ID.toString(), SPEC_JOB_CONFIG, true);
      }

      final long otherConnectionJobId = jobPersistence.enqueueJob(UUID.randomUUID().toString(), SPEC_JOB_CONFIG, true).orElseThrow();

      final List<Job> actualList =
          jobPersistence.listJobsIncludingId(Set.of(SPEC_JOB_CONFIG.getConfigType()), CONNECTION_ID.toString(), otherConnectionJobId, 25);
      assertEquals(List.of(), actualList);
    }

  }

  @Nested
  @DisplayName("When listing job with status")
  class ListJobsWithStatus {

    @Test
    @DisplayName("Should only list jobs for the requested connection and with the requested statuses and config types")
    void testListJobsWithStatusesAndConfigTypesForConnection() throws IOException, InterruptedException {
      final UUID desiredConnectionId = UUID.randomUUID();
      final UUID otherConnectionId = UUID.randomUUID();

      // desired connection, statuses, and config types
      final long desiredJobId1 = jobPersistence.enqueueJob(desiredConnectionId.toString(), SYNC_JOB_CONFIG, true).orElseThrow();
      jobPersistence.succeedAttempt(desiredJobId1, jobPersistence.createAttempt(desiredJobId1, LOG_PATH));
      final long desiredJobId2 = jobPersistence.enqueueJob(desiredConnectionId.toString(), SYNC_JOB_CONFIG, true).orElseThrow();
      final long desiredJobId3 = jobPersistence.enqueueJob(desiredConnectionId.toString(), CHECK_JOB_CONFIG, true).orElseThrow();
      jobPersistence.succeedAttempt(desiredJobId3, jobPersistence.createAttempt(desiredJobId3, LOG_PATH));
      final long desiredJobId4 = jobPersistence.enqueueJob(desiredConnectionId.toString(), CHECK_JOB_CONFIG, true).orElseThrow();

      // right connection id and status, wrong config type
      jobPersistence.enqueueJob(desiredConnectionId.toString(), SPEC_JOB_CONFIG, true).orElseThrow();
      // right config type and status, wrong connection id
      jobPersistence.enqueueJob(otherConnectionId.toString(), SYNC_JOB_CONFIG, true).orElseThrow();
      // right connection id and config type, wrong status
      final long otherJobId3 = jobPersistence.enqueueJob(desiredConnectionId.toString(), CHECK_JOB_CONFIG, true).orElseThrow();
      jobPersistence.failAttempt(otherJobId3, jobPersistence.createAttempt(otherJobId3, LOG_PATH));

      final List<Job> actualJobs = jobPersistence.listJobsForConnectionWithStatuses(desiredConnectionId,
          Set.of(ConfigType.SYNC, ConfigType.CHECK_CONNECTION_DESTINATION), Set.of(JobStatus.PENDING, JobStatus.SUCCEEDED));

      final Job expectedDesiredJob1 = createJob(desiredJobId1, SYNC_JOB_CONFIG, JobStatus.SUCCEEDED,
          Lists.newArrayList(createAttempt(0, desiredJobId1, AttemptStatus.SUCCEEDED, LOG_PATH)),
          NOW.getEpochSecond(), desiredConnectionId.toString(), true);
      final Job expectedDesiredJob2 =
          createJob(desiredJobId2, SYNC_JOB_CONFIG, JobStatus.PENDING, Lists.newArrayList(), NOW.getEpochSecond(), desiredConnectionId.toString(),
              true);
      final Job expectedDesiredJob3 = createJob(desiredJobId3, CHECK_JOB_CONFIG, JobStatus.SUCCEEDED,
          Lists.newArrayList(createAttempt(0, desiredJobId3, AttemptStatus.SUCCEEDED, LOG_PATH)),
          NOW.getEpochSecond(), desiredConnectionId.toString(), true);
      final Job expectedDesiredJob4 =
          createJob(desiredJobId4, CHECK_JOB_CONFIG, JobStatus.PENDING, Lists.newArrayList(), NOW.getEpochSecond(), desiredConnectionId.toString(),
              true);

      assertEquals(Sets.newHashSet(expectedDesiredJob1, expectedDesiredJob2, expectedDesiredJob3, expectedDesiredJob4), Sets.newHashSet(actualJobs));
    }

  }

  @Nested
  @DisplayName("When resetting job")
  class ResetJob {

    @Test
    @DisplayName("Should reset job and put job in pending state")
    void testResetJob() throws IOException {
      final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();
      final int attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH);
      final Job created = jobPersistence.getJob(jobId);

      jobPersistence.failAttempt(jobId, attemptNumber);
      when(timeSupplier.get()).thenReturn(Instant.ofEpochMilli(4242));
      jobPersistence.resetJob(jobId);

      final Job updated = jobPersistence.getJob(jobId);
      assertEquals(JobStatus.PENDING, updated.getStatus());
      assertNotEquals(created.getUpdatedAtInSecond(), updated.getUpdatedAtInSecond());
    }

    @Test
    @DisplayName("Should not be able to reset a cancelled job")
    void testResetJobCancelled() throws IOException {
      final long jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow();

      jobPersistence.cancelJob(jobId);
      assertDoesNotThrow(() -> jobPersistence.resetJob(jobId));

      final Job updated = jobPersistence.getJob(jobId);
      assertEquals(JobStatus.CANCELLED, updated.getStatus());
    }

  }

}
