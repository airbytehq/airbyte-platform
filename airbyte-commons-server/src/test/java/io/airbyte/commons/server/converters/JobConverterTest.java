/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.AttemptFailureSummary;
import io.airbyte.api.model.generated.AttemptInfoRead;
import io.airbyte.api.model.generated.AttemptInfoReadLogs;
import io.airbyte.api.model.generated.AttemptRead;
import io.airbyte.api.model.generated.AttemptStats;
import io.airbyte.api.model.generated.AttemptStreamStats;
import io.airbyte.api.model.generated.DestinationDefinitionRead;
import io.airbyte.api.model.generated.JobConfigType;
import io.airbyte.api.model.generated.JobDebugRead;
import io.airbyte.api.model.generated.JobInfoLightRead;
import io.airbyte.api.model.generated.JobInfoRead;
import io.airbyte.api.model.generated.JobRead;
import io.airbyte.api.model.generated.JobRefreshConfig;
import io.airbyte.api.model.generated.JobWithAttemptsRead;
import io.airbyte.api.model.generated.LogFormatType;
import io.airbyte.api.model.generated.ResetConfig;
import io.airbyte.api.model.generated.SourceDefinitionRead;
import io.airbyte.api.model.generated.StreamDescriptor;
import io.airbyte.api.model.generated.SynchronousJobRead;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.logging.LogClientManager;
import io.airbyte.commons.logging.LogEvent;
import io.airbyte.commons.logging.LogEvents;
import io.airbyte.commons.logging.LogSource;
import io.airbyte.commons.logging.LogUtils;
import io.airbyte.commons.server.scheduler.SynchronousJobMetadata;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.config.AirbyteStream;
import io.airbyte.config.Attempt;
import io.airbyte.config.AttemptStatus;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.DestinationSyncMode;
import io.airbyte.config.FailureReason;
import io.airbyte.config.FailureReason.FailureOrigin;
import io.airbyte.config.FailureReason.FailureType;
import io.airbyte.config.Job;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.JobOutput;
import io.airbyte.config.JobOutput.OutputType;
import io.airbyte.config.JobResetConnectionConfig;
import io.airbyte.config.JobStatus;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.RefreshConfig;
import io.airbyte.config.RefreshStream;
import io.airbyte.config.ResetSourceConfiguration;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.StandardSyncSummary;
import io.airbyte.config.StreamSyncStats;
import io.airbyte.config.SyncMode;
import io.airbyte.config.SyncStats;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class JobConverterTest {

  private JobConverter jobConverter;
  private LogClientManager logClientManager;
  private LogUtils logUtils;
  private static final long CREATED_AT = System.currentTimeMillis() / 1000;
  private static final Path LOG_PATH = Path.of("log_path");
  private static final String FAILURE_EXTERNAL_MESSAGE = "something went wrong";
  private static final long FAILURE_TIMESTAMP = System.currentTimeMillis();
  private static final String FAILURE_STACKTRACE = "stacktrace";
  private static final String USERS = "users";
  private static final String ACCOUNTS = "accounts";
  private static final FailureReason FAILURE_REASON = new FailureReason()
      .withFailureOrigin(FailureOrigin.SOURCE)
      .withFailureType(FailureType.SYSTEM_ERROR)
      .withExternalMessage(FAILURE_EXTERNAL_MESSAGE)
      .withStacktrace(FAILURE_STACKTRACE)
      .withTimestamp(FAILURE_TIMESTAMP);

  @Test
  void testEnumConversion() {
    assertTrue(Enums.isCompatible(JobConfig.ConfigType.class, JobConfigType.class));
    assertTrue(Enums.isCompatible(JobStatus.class, io.airbyte.api.model.generated.JobStatus.class));
    assertTrue(Enums.isCompatible(AttemptStatus.class, io.airbyte.api.model.generated.AttemptStatus.class));
    assertTrue(Enums.isCompatible(FailureReason.FailureOrigin.class, io.airbyte.api.model.generated.FailureOrigin.class));
  }

  private static Stream<Arguments> getExtractRefreshScenarios() {
    return Stream.of(Arguments.of(
        new Job(1, ConfigType.SYNC, null, null, null, null, null, 13, 37), Optional.empty()),
        Arguments.of(
            new Job(1, ConfigType.RESET_CONNECTION, null, null, null, null, null, 13, 37), Optional.empty()),
        Arguments.of(
            new Job(1, ConfigType.REFRESH, null, new JobConfig()
                .withRefresh(new RefreshConfig().withStreamsToRefresh(
                    List.of(new RefreshStream().withStreamDescriptor(new io.airbyte.config.StreamDescriptor().withName("test"))))),
                null, null, null, 13, 37),
            Optional.of(new JobRefreshConfig().streamsToRefresh(List.of(new StreamDescriptor().name("test"))))),
        Arguments.of(
            new Job(1, ConfigType.REFRESH, null, new JobConfig()
                .withRefresh(new RefreshConfig().withStreamsToRefresh(
                    List.of(new RefreshStream().withStreamDescriptor(null)))),
                null, null, null, 13, 37),
            Optional.empty()));
  }

  @ParameterizedTest
  @MethodSource("getExtractRefreshScenarios")
  void testExtractRefresh(final Job job, final Optional<JobRefreshConfig> expectedConfig) {
    final Optional<JobRefreshConfig> actualConfig = JobConverter.extractRefreshConfigIfNeeded(job);

    assertEquals(expectedConfig, actualConfig);
  }

  @Nested
  class TestJob {

    private static final long JOB_ID = 100L;
    private static final Integer ATTEMPT_NUMBER = 0;
    private static final String JOB_CONFIG_ID = "123";
    private static final JobStatus JOB_STATUS = JobStatus.RUNNING;
    private static final AttemptStatus ATTEMPT_STATUS = AttemptStatus.RUNNING;
    private static final JobConfig.ConfigType CONFIG_TYPE = ConfigType.SYNC;
    private static final long RECORDS_EMITTED = 15L;
    private static final long BYTES_EMITTED = 100L;
    private static final long RECORDS_COMMITTED = 10L;
    private static final long STATE_MESSAGES_EMITTED = 2L;
    private static final String STREAM_NAME = "stream1";
    private static final boolean PARTIAL_SUCCESS = false;

    private static final JobConfig JOB_CONFIG = new JobConfig()
        .withConfigType(CONFIG_TYPE)
        .withSync(new JobSyncConfig().withConfiguredAirbyteCatalog(new ConfiguredAirbyteCatalog().withStreams(List.of(
            new ConfiguredAirbyteStream(new AirbyteStream(USERS, Jsons.emptyObject(), List.of(SyncMode.INCREMENTAL)), SyncMode.INCREMENTAL,
                DestinationSyncMode.APPEND),
            new ConfiguredAirbyteStream(new AirbyteStream(ACCOUNTS, Jsons.emptyObject(), List.of(SyncMode.INCREMENTAL)), SyncMode.INCREMENTAL,
                DestinationSyncMode.APPEND)))));

    private static final JobOutput JOB_OUTPUT = new JobOutput()
        .withOutputType(OutputType.SYNC)
        .withSync(new StandardSyncOutput()
            .withStandardSyncSummary(new StandardSyncSummary()
                .withRecordsSynced(RECORDS_EMITTED)
                .withBytesSynced(BYTES_EMITTED)
                .withTotalStats(new SyncStats()
                    .withRecordsEmitted(RECORDS_EMITTED)
                    .withBytesEmitted(BYTES_EMITTED)
                    .withSourceStateMessagesEmitted(STATE_MESSAGES_EMITTED)
                    .withRecordsCommitted(RECORDS_COMMITTED))
                .withStreamStats(List.of(new StreamSyncStats()
                    .withStreamName(STREAM_NAME)
                    .withStats(new SyncStats()
                        .withRecordsEmitted(RECORDS_EMITTED)
                        .withBytesEmitted(BYTES_EMITTED)
                        .withSourceStateMessagesEmitted(STATE_MESSAGES_EMITTED)
                        .withRecordsCommitted(RECORDS_COMMITTED))))));

    private Job job;

    private static final JobInfoRead JOB_INFO_UNSTRUCTURED_LOGS =
        new JobInfoRead()
            .job(new JobRead()
                .id(JOB_ID)
                .configId(JOB_CONFIG_ID)
                .status(io.airbyte.api.model.generated.JobStatus.RUNNING)
                .configType(JobConfigType.SYNC)
                .enabledStreams(List.of(new StreamDescriptor().name(USERS), new StreamDescriptor().name(ACCOUNTS)))
                .createdAt(CREATED_AT)
                .updatedAt(CREATED_AT))
            .attempts(List.of(new AttemptInfoRead()
                .attempt(new AttemptRead()
                    .id((long) ATTEMPT_NUMBER)
                    .status(io.airbyte.api.model.generated.AttemptStatus.RUNNING)
                    .recordsSynced(RECORDS_EMITTED)
                    .bytesSynced(BYTES_EMITTED)
                    .totalStats(new AttemptStats()
                        .recordsEmitted(RECORDS_EMITTED)
                        .bytesEmitted(BYTES_EMITTED)
                        .stateMessagesEmitted(STATE_MESSAGES_EMITTED)
                        .recordsCommitted(RECORDS_COMMITTED))
                    .streamStats(List.of(new AttemptStreamStats()
                        .streamName(STREAM_NAME)
                        .stats(new AttemptStats()
                            .recordsEmitted(RECORDS_EMITTED)
                            .bytesEmitted(BYTES_EMITTED)
                            .stateMessagesEmitted(STATE_MESSAGES_EMITTED)
                            .recordsCommitted(RECORDS_COMMITTED))))
                    .updatedAt(CREATED_AT)
                    .createdAt(CREATED_AT)
                    .endedAt(CREATED_AT)
                    .failureSummary(new AttemptFailureSummary()
                        .failures(List.of(new io.airbyte.api.model.generated.FailureReason()
                            .failureOrigin(io.airbyte.api.model.generated.FailureOrigin.SOURCE)
                            .failureType(io.airbyte.api.model.generated.FailureType.SYSTEM_ERROR)
                            .externalMessage(FAILURE_EXTERNAL_MESSAGE)
                            .stacktrace(FAILURE_STACKTRACE)
                            .timestamp(FAILURE_TIMESTAMP)))
                        .partialSuccess(PARTIAL_SUCCESS)))
                .logType(LogFormatType.FORMATTED)
                .logs(new AttemptInfoReadLogs().logLines(List.of()))));

    private static final String version = "0.33.4";
    private static final AirbyteVersion airbyteVersion = new AirbyteVersion(version);
    private static final SourceDefinitionRead sourceDefinitionRead = new SourceDefinitionRead().sourceDefinitionId(UUID.randomUUID());
    private static final DestinationDefinitionRead destinationDefinitionRead =
        new DestinationDefinitionRead().destinationDefinitionId(UUID.randomUUID());

    private static final JobDebugRead JOB_DEBUG_INFO =
        new JobDebugRead()
            .id(JOB_ID)
            .configId(JOB_CONFIG_ID)
            .status(io.airbyte.api.model.generated.JobStatus.RUNNING)
            .configType(JobConfigType.SYNC)
            .airbyteVersion(airbyteVersion.serialize())
            .sourceDefinition(sourceDefinitionRead)
            .destinationDefinition(destinationDefinitionRead);

    private static final JobWithAttemptsRead JOB_WITH_ATTEMPTS_READ_UNSTRUCTURED_LOGS = new JobWithAttemptsRead()
        .job(JOB_INFO_UNSTRUCTURED_LOGS.getJob())
        .attempts(JOB_INFO_UNSTRUCTURED_LOGS.getAttempts().stream().map(AttemptInfoRead::getAttempt).collect(Collectors.toList()));

    private static final io.airbyte.config.AttemptFailureSummary FAILURE_SUMMARY = new io.airbyte.config.AttemptFailureSummary()
        .withFailures(List.of(FAILURE_REASON))
        .withPartialSuccess(PARTIAL_SUCCESS);

    @BeforeEach
    public void setUp() {
      logClientManager = mock(LogClientManager.class);
      logUtils = mock(LogUtils.class);
      jobConverter = new JobConverter(logClientManager, logUtils);
      job = mock(Job.class);
      final Attempt attempt = mock(Attempt.class);
      when(job.getId()).thenReturn(JOB_ID);
      when(job.getConfigType()).thenReturn(JOB_CONFIG.getConfigType());
      when(job.getScope()).thenReturn(JOB_CONFIG_ID);
      when(job.getConfig()).thenReturn(JOB_CONFIG);
      when(job.getStatus()).thenReturn(JOB_STATUS);
      when(job.getCreatedAtInSecond()).thenReturn(CREATED_AT);
      when(job.getUpdatedAtInSecond()).thenReturn(CREATED_AT);
      when(job.getAttempts()).thenReturn(List.of(attempt));
      when(attempt.getAttemptNumber()).thenReturn(ATTEMPT_NUMBER);
      when(attempt.getStatus()).thenReturn(ATTEMPT_STATUS);
      when(attempt.getOutput()).thenReturn(Optional.of(JOB_OUTPUT));
      when(attempt.getLogPath()).thenReturn(LOG_PATH);
      when(attempt.getCreatedAtInSecond()).thenReturn(CREATED_AT);
      when(attempt.getUpdatedAtInSecond()).thenReturn(CREATED_AT);
      when(attempt.getEndedAtInSecond()).thenReturn(Optional.of(CREATED_AT));
      when(attempt.getFailureSummary()).thenReturn(Optional.of(FAILURE_SUMMARY));
    }

    @Test
    void testGetJobInfoRead() {
      when(logClientManager.getLogs(any())).thenReturn(new LogEvents(List.of(), "1"));
      assertEquals(JOB_INFO_UNSTRUCTURED_LOGS, jobConverter.getJobInfoRead(job));
    }

    @Test
    void testGetJobInfoLightRead() {
      final JobInfoLightRead expected = new JobInfoLightRead().job(JOB_INFO_UNSTRUCTURED_LOGS.getJob());
      assertEquals(expected, jobConverter.getJobInfoLightRead(job));
    }

    @Test
    void testGetDebugJobInfoRead() {
      assertEquals(JOB_DEBUG_INFO,
          JobConverter.getDebugJobInfoRead(JOB_INFO_UNSTRUCTURED_LOGS, sourceDefinitionRead, destinationDefinitionRead, airbyteVersion));
    }

    @Test
    void testGetJobWithAttemptsRead() {
      assertEquals(JOB_WITH_ATTEMPTS_READ_UNSTRUCTURED_LOGS, JobConverter.getJobWithAttemptsRead(job));
    }

    @Test
    void testGetJobWithAttemptsReadStructuredLogs() {
      final String logEventVersion = "1";
      when(logClientManager.getLogs(any())).thenReturn(
          new LogEvents(List.of(new LogEvent(System.currentTimeMillis(), "message", "INFO", LogSource.PLATFORM, null, null)), logEventVersion));
      final JobInfoRead jobInfoRead = jobConverter.getJobInfoRead(job);
      assertEquals(LogFormatType.STRUCTURED, jobInfoRead.getAttempts().getFirst().getLogType());
      assertEquals(logEventVersion, jobInfoRead.getAttempts().getFirst().getLogs().getVersion());
      assertEquals(1, jobInfoRead.getAttempts().getFirst().getLogs().getEvents().size());
    }

    // this test intentionally only looks at the reset config as the rest is the same here.
    @Test
    void testResetJobIncludesResetConfig() {
      final JobConfig resetConfig = new JobConfig()
          .withConfigType(ConfigType.RESET_CONNECTION)
          .withResetConnection(new JobResetConnectionConfig().withResetSourceConfiguration(new ResetSourceConfiguration().withStreamsToReset(List.of(
              new io.airbyte.config.StreamDescriptor().withName(USERS),
              new io.airbyte.config.StreamDescriptor().withName(ACCOUNTS)))));
      final Job resetJob = new Job(
          JOB_ID,
          ConfigType.RESET_CONNECTION,
          JOB_CONFIG_ID,
          resetConfig,
          Collections.emptyList(),
          JobStatus.SUCCEEDED,
          CREATED_AT,
          CREATED_AT,
          CREATED_AT);

      final ResetConfig expectedResetConfig = new ResetConfig().streamsToReset(List.of(
          new StreamDescriptor().name(USERS),
          new StreamDescriptor().name(ACCOUNTS)));
      assertEquals(expectedResetConfig, jobConverter.getJobInfoRead(resetJob).getJob().getResetConfig());
    }

    @Test
    void testResetJobExcludesConfigIfNull() {
      final JobConfig resetConfig = new JobConfig()
          .withConfigType(ConfigType.RESET_CONNECTION)
          .withResetConnection(new JobResetConnectionConfig().withResetSourceConfiguration(null));
      final Job resetJob = new Job(
          JOB_ID,
          ConfigType.RESET_CONNECTION,
          JOB_CONFIG_ID,
          resetConfig,
          Collections.emptyList(),
          JobStatus.SUCCEEDED,
          CREATED_AT,
          CREATED_AT,
          CREATED_AT);

      assertNull(jobConverter.getJobInfoRead(resetJob).getJob().getResetConfig());
    }

  }

  @Nested
  class TestSynchronousJob {

    private SynchronousJobMetadata metadata;
    private static final UUID JOB_ID = UUID.randomUUID();
    private static final JobConfig.ConfigType CONFIG_TYPE = ConfigType.DISCOVER_SCHEMA;
    private static final Optional<UUID> CONFIG_ID = Optional.empty();
    private static final boolean JOB_SUCCEEDED = false;
    private static final boolean CONNECTOR_CONFIG_UPDATED = false;
    private static final SynchronousJobRead SYNCHRONOUS_JOB_INFO_UNSTRUCTURED_LOGS = new SynchronousJobRead()
        .id(JOB_ID)
        .configType(JobConfigType.DISCOVER_SCHEMA)
        .configId(String.valueOf(CONFIG_ID))
        .createdAt(CREATED_AT)
        .endedAt(CREATED_AT)
        .succeeded(JOB_SUCCEEDED)
        .connectorConfigurationUpdated(CONNECTOR_CONFIG_UPDATED)
        .logType(LogFormatType.FORMATTED)
        .logs(new AttemptInfoReadLogs().logLines(new ArrayList<>()))
        .failureReason(new io.airbyte.api.model.generated.FailureReason()
            .failureOrigin(io.airbyte.api.model.generated.FailureOrigin.SOURCE)
            .failureType(io.airbyte.api.model.generated.FailureType.SYSTEM_ERROR)
            .externalMessage(FAILURE_EXTERNAL_MESSAGE)
            .stacktrace(FAILURE_STACKTRACE)
            .timestamp(FAILURE_TIMESTAMP));

    @BeforeEach
    public void setUp() {
      logClientManager = mock(LogClientManager.class);
      jobConverter = new JobConverter(logClientManager, logUtils);
      metadata = mock(SynchronousJobMetadata.class);
      when(metadata.getId()).thenReturn(JOB_ID);
      when(metadata.getConfigType()).thenReturn(CONFIG_TYPE);
      when(metadata.getConfigId()).thenReturn(CONFIG_ID);
      when(metadata.getCreatedAt()).thenReturn(CREATED_AT);
      when(metadata.getEndedAt()).thenReturn(CREATED_AT);
      when(metadata.isSucceeded()).thenReturn(JOB_SUCCEEDED);
      when(metadata.isConnectorConfigurationUpdated()).thenReturn(CONNECTOR_CONFIG_UPDATED);
      when(metadata.getLogPath()).thenReturn(LOG_PATH);
      when(metadata.getFailureReason()).thenReturn(FAILURE_REASON);
    }

    @Test
    void testSynchronousJobRead() {
      when(logClientManager.getLogs(any())).thenReturn(new LogEvents(List.of(), "1"));
      assertEquals(SYNCHRONOUS_JOB_INFO_UNSTRUCTURED_LOGS, jobConverter.getSynchronousJobRead(metadata));
    }

  }

}
