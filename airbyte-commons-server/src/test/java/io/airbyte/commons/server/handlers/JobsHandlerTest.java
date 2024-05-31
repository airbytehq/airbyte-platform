/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.config.JobConfig.ConfigType.RESET_CONNECTION;
import static io.airbyte.config.JobConfig.ConfigType.SYNC;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.InternalOperationResult;
import io.airbyte.api.model.generated.JobFailureRequest;
import io.airbyte.api.model.generated.JobSuccessWithAttemptNumberRequest;
import io.airbyte.commons.server.JobStatus;
import io.airbyte.commons.server.errors.BadRequestException;
import io.airbyte.commons.server.handlers.helpers.JobCreationAndStatusUpdateHelper;
import io.airbyte.config.AttemptFailureSummary;
import io.airbyte.config.AttemptSyncConfig;
import io.airbyte.config.FailureReason;
import io.airbyte.config.FailureReason.FailureOrigin;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobOutput;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.NormalizationSummary;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.StandardSyncSummary;
import io.airbyte.config.StandardSyncSummary.ReplicationStatus;
import io.airbyte.config.StreamSyncStats;
import io.airbyte.config.SyncStats;
import io.airbyte.data.services.ConnectionTimelineEventService;
import io.airbyte.persistence.job.JobNotifier;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.errorreporter.AttemptConfigReportingContext;
import io.airbyte.persistence.job.errorreporter.JobErrorReporter;
import io.airbyte.persistence.job.errorreporter.SyncJobReportingContext;
import io.airbyte.persistence.job.models.Attempt;
import io.airbyte.persistence.job.models.Job;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.SyncMode;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

/**
 * JobsHandlerTest.
 */
@SuppressWarnings("PMD")
public class JobsHandlerTest {

  private JobPersistence jobPersistence;
  private JobNotifier jobNotifier;
  private JobsHandler jobsHandler;
  private JobCreationAndStatusUpdateHelper helper;
  private JobErrorReporter jobErrorReporter;
  private ConnectionTimelineEventService connectionTimelineEventService;

  private static final long JOB_ID = 12;
  private static final int ATTEMPT_NUMBER = 1;
  private static final UUID CONNECTION_ID = UUID.randomUUID();
  private static final StandardSyncOutput standardSyncOutput = new StandardSyncOutput()
      .withStandardSyncSummary(
          new StandardSyncSummary()
              .withStatus(ReplicationStatus.COMPLETED))
      .withNormalizationSummary(
          new NormalizationSummary());

  private static final JobOutput jobOutput = new JobOutput().withSync(standardSyncOutput);
  private static final ConfiguredAirbyteCatalog catalog = new ConfiguredAirbyteCatalog()
      .withStreams(List.of(new ConfiguredAirbyteStream().withSyncMode(SyncMode.FULL_REFRESH).withStream(new AirbyteStream().withName("stream"))));
  private static final JobConfig simpleConfig =
      new JobConfig().withConfigType(SYNC).withSync(new JobSyncConfig().withConfiguredAirbyteCatalog(catalog));

  private static final AttemptFailureSummary failureSummary = new AttemptFailureSummary()
      .withFailures(Collections.singletonList(
          new FailureReason()
              .withFailureOrigin(FailureOrigin.SOURCE)));

  @BeforeEach
  void beforeEach() {
    jobPersistence = mock(JobPersistence.class);
    jobNotifier = mock(JobNotifier.class);
    jobErrorReporter = mock(JobErrorReporter.class);
    connectionTimelineEventService = mock(ConnectionTimelineEventService.class);

    helper = mock(JobCreationAndStatusUpdateHelper.class);
    jobsHandler = new JobsHandler(jobPersistence, helper, jobNotifier, jobErrorReporter, connectionTimelineEventService);
  }

  @Test
  void testJobSuccessWithAttemptNumber() throws IOException {
    final var request = new JobSuccessWithAttemptNumberRequest()
        .attemptNumber(ATTEMPT_NUMBER)
        .jobId(JOB_ID)
        .connectionId(CONNECTION_ID)
        .standardSyncOutput(standardSyncOutput);

    final Job job =
        new Job(JOB_ID, SYNC, CONNECTION_ID.toString(), simpleConfig, List.of(), io.airbyte.persistence.job.models.JobStatus.SUCCEEDED, 0L, 0, 0);
    when(jobPersistence.getJob(JOB_ID)).thenReturn(job);
    jobsHandler.jobSuccessWithAttemptNumber(request);

    verify(jobPersistence).writeOutput(JOB_ID, ATTEMPT_NUMBER, jobOutput);
    verify(jobPersistence).succeedAttempt(JOB_ID, ATTEMPT_NUMBER);
    verify(jobNotifier).successJob(any(), any());
    verify(helper).trackCompletion(any(), eq(JobStatus.SUCCEEDED));
    verify(connectionTimelineEventService).writeEvent(eq(CONNECTION_ID), any());
  }

  @Test
  void testGetLoadedStats() {

    final String userStreamName = "user";
    final SyncMode userStreamMode = SyncMode.FULL_REFRESH;
    final String purchaseStreamName = "purchase";
    final SyncMode purchaseStreamMode = SyncMode.INCREMENTAL;
    final String vendorStreamName = "vendor";
    final SyncMode vendorStreamMode = SyncMode.INCREMENTAL;

    final ConfiguredAirbyteCatalog catalog = new ConfiguredAirbyteCatalog()
        .withStreams(List.of(
            new ConfiguredAirbyteStream().withSyncMode(userStreamMode).withStream(new AirbyteStream().withName(userStreamName)),
            new ConfiguredAirbyteStream().withSyncMode(purchaseStreamMode).withStream(new AirbyteStream().withName(purchaseStreamName)),
            new ConfiguredAirbyteStream().withSyncMode(vendorStreamMode).withStream(new AirbyteStream().withName(vendorStreamName))));

    final JobConfig jobConfig = new JobConfig().withConfigType(SYNC).withSync(new JobSyncConfig().withConfiguredAirbyteCatalog(catalog));
    final Job job =
        new Job(100L, SYNC, CONNECTION_ID.toString(), jobConfig, List.of(), io.airbyte.persistence.job.models.JobStatus.SUCCEEDED, 0L, 0L, 0L);

    /*
     * on a per stream basis, the stats are "users" -> (100L, 1L), (500L, 8L), (200L, 7L) "purchase" ->
     * (1000L, 10L), (5000L, 80L), (2000L, 70L) "vendor" -> (10000L, 100L), (50000L, 800L), (20000L,
     * 700L)
     */

    final List<Map<String, SyncStats>> perAttemptStreamStats = List.of(
        Map.of(
            userStreamName, new SyncStats().withBytesCommitted(100L).withRecordsCommitted(1L),
            purchaseStreamName, new SyncStats().withBytesCommitted(1000L).withRecordsCommitted(10L),
            vendorStreamName, new SyncStats().withBytesCommitted(10000L).withRecordsCommitted(100L)),

        Map.of(
            userStreamName, new SyncStats().withBytesCommitted(500L).withRecordsCommitted(8L),
            purchaseStreamName, new SyncStats().withBytesCommitted(5000L).withRecordsCommitted(80L),
            vendorStreamName, new SyncStats().withBytesCommitted(50000L).withRecordsCommitted(800L)),
        Map.of(
            userStreamName, new SyncStats().withBytesCommitted(200L).withRecordsCommitted(7L),
            purchaseStreamName, new SyncStats().withBytesCommitted(2000L).withRecordsCommitted(70L),
            vendorStreamName, new SyncStats().withBytesCommitted(20000L).withRecordsCommitted(700L)));

    final List<JobPersistence.AttemptStats> attemptStatsList = perAttemptStreamStats
        .stream().map(dict -> new JobPersistence.AttemptStats(
            new SyncStats(),
            dict.entrySet()
                .stream().map(entry -> new StreamSyncStats()
                    .withStreamName(entry.getKey()).withStats(entry.getValue()))
                .toList()))
        .toList();

    // For full refresh streams, on the last value matters, for other modes the bytes/records are summed
    // across syncs
    final long expectedBytesLoaded = 200L + (1000L + 5000L + 2000L) + (10000L + 50000L + 20000L);
    final long expectedRecordsLoaded = 7L + (10L + 80L + 70L) + (100L + 800L + 700L);
    final var result = jobsHandler.buildLoadedStats(job, attemptStatsList);
    assertEquals(expectedBytesLoaded, result.bytes());
    assertEquals(expectedRecordsLoaded, result.records());
  }

  @Test
  void testResetJobNoNotification() throws IOException {
    final var request = new JobSuccessWithAttemptNumberRequest()
        .attemptNumber(ATTEMPT_NUMBER)
        .jobId(JOB_ID)
        .connectionId(UUID.randomUUID())
        .standardSyncOutput(standardSyncOutput);
    Job job = new Job(JOB_ID, RESET_CONNECTION, "", simpleConfig, List.of(), io.airbyte.persistence.job.models.JobStatus.SUCCEEDED, 0L, 0, 0);
    when(jobPersistence.getJob(JOB_ID)).thenReturn(job);
    jobsHandler.jobSuccessWithAttemptNumber(request);

    verify(jobNotifier, never()).successJob(any(), any());
  }

  @Test
  void setJobSuccessWrapException() throws IOException {
    final IOException exception = new IOException("oops");
    Mockito.doThrow(exception)
        .when(jobPersistence).succeedAttempt(JOB_ID, ATTEMPT_NUMBER);

    Assertions
        .assertThatThrownBy(
            () -> jobsHandler.jobSuccessWithAttemptNumber(new JobSuccessWithAttemptNumberRequest()
                .attemptNumber(ATTEMPT_NUMBER)
                .jobId(JOB_ID)
                .connectionId(CONNECTION_ID)
                .standardSyncOutput(standardSyncOutput)))
        .isInstanceOf(RuntimeException.class)
        .hasCauseInstanceOf(IOException.class);

    verify(helper, times(1)).trackCompletionForInternalFailure(JOB_ID, CONNECTION_ID, ATTEMPT_NUMBER, JobStatus.SUCCEEDED, exception);
  }

  @Test
  void didPreviousJobSucceedReturnsFalseIfNoPreviousJob() throws Exception {
    when(jobPersistence.listJobsIncludingId(any(), any(), anyLong(), anyInt()))
        .thenReturn(List.of(
            Mockito.mock(Job.class),
            Mockito.mock(Job.class),
            Mockito.mock(Job.class)));

    when(helper.findPreviousJob(any(), anyLong()))
        .thenReturn(Optional.empty());

    when(helper.didJobSucceed(any()))
        .thenReturn(true);

    final var result = jobsHandler.didPreviousJobSucceed(UUID.randomUUID(), 123);
    assertFalse(result.getValue());
  }

  @Test
  void didPreviousJobSucceedReturnsTrueIfPreviousJobSucceeded() throws Exception {
    when(jobPersistence.listJobsIncludingId(any(), any(), anyLong(), anyInt()))
        .thenReturn(List.of(
            Mockito.mock(Job.class),
            Mockito.mock(Job.class),
            Mockito.mock(Job.class)));

    when(helper.findPreviousJob(any(), anyLong()))
        .thenReturn(Optional.of(Mockito.mock(Job.class)));

    when(helper.didJobSucceed(any()))
        .thenReturn(true);

    final var result = jobsHandler.didPreviousJobSucceed(UUID.randomUUID(), 123);
    assertTrue(result.getValue());
  }

  @Test
  void didPreviousJobSucceedReturnsFalseIfPreviousJobNotInSucceededState() throws Exception {
    when(jobPersistence.listJobsIncludingId(any(), any(), anyLong(), anyInt()))
        .thenReturn(List.of(
            Mockito.mock(Job.class),
            Mockito.mock(Job.class),
            Mockito.mock(Job.class)));

    when(helper.findPreviousJob(any(), anyLong()))
        .thenReturn(Optional.of(Mockito.mock(Job.class)));

    when(helper.didJobSucceed(any()))
        .thenReturn(false);

    final var result = jobsHandler.didPreviousJobSucceed(UUID.randomUUID(), 123);
    assertFalse(result.getValue());
  }

  @Test
  void persistJobCancellationSuccess() throws Exception {
    final var mockJob = Mockito.mock(Job.class);
    when(jobPersistence.getJob(JOB_ID)).thenReturn(mockJob);

    jobsHandler.persistJobCancellation(CONNECTION_ID, JOB_ID, ATTEMPT_NUMBER, failureSummary);

    verify(jobPersistence).failAttempt(JOB_ID, ATTEMPT_NUMBER);
    verify(jobPersistence).writeAttemptFailureSummary(JOB_ID, ATTEMPT_NUMBER, failureSummary);
    verify(jobPersistence).cancelJob(JOB_ID);
    verify(helper).trackCompletion(any(), eq(JobStatus.FAILED));
  }

  @Test
  void persistJobCancellationIOException() throws Exception {
    final var exception = new IOException("bang.");
    when(jobPersistence.getJob(JOB_ID)).thenThrow(exception);

    // map to runtime exception
    assertThrows(RuntimeException.class, () -> jobsHandler.persistJobCancellation(CONNECTION_ID, JOB_ID, ATTEMPT_NUMBER, failureSummary));
    // emit analytics
    verify(helper).trackCompletionForInternalFailure(JOB_ID, CONNECTION_ID, ATTEMPT_NUMBER, JobStatus.FAILED, exception);
  }

  @ParameterizedTest
  @MethodSource("randomObjects")
  void persistJobCancellationValidatesFailureSummary(final Object thing) {
    assertThrows(BadRequestException.class, () -> jobsHandler.persistJobCancellation(CONNECTION_ID, JOB_ID, ATTEMPT_NUMBER, thing));
  }

  private static Stream<Arguments> randomObjects() {
    return Stream.of(
        Arguments.of(123L),
        Arguments.of(true),
        Arguments.of(List.of("123", "123")),
        Arguments.of("a string"),
        Arguments.of(543.0));
  }

  @Test
  void testReportJobStart() throws IOException {
    final Long jobId = 5L;
    final var result = jobsHandler.reportJobStart(jobId);
    assertEquals(new InternalOperationResult().succeeded(true), result);
    verify(helper).reportJobStart(jobId);
  }

  @Test
  void setJobFailure() throws IOException {
    final AttemptFailureSummary failureSummary = new AttemptFailureSummary()
        .withFailures(Collections.singletonList(
            new FailureReason()
                .withFailureOrigin(FailureOrigin.SOURCE)));
    final String failureReason = "reason";

    final Attempt mAttempt = Mockito.mock(Attempt.class);
    when(mAttempt.getFailureSummary()).thenReturn(Optional.of(failureSummary));

    final JobSyncConfig jobSyncConfig = new JobSyncConfig()
        .withSourceDefinitionVersionId(UUID.randomUUID())
        .withDestinationDefinitionVersionId(UUID.randomUUID());

    final AttemptSyncConfig mSyncConfig = Mockito.mock(AttemptSyncConfig.class);
    when(mAttempt.getSyncConfig()).thenReturn(Optional.of(mSyncConfig));

    final JobConfig mJobConfig = Mockito.mock(JobConfig.class);
    when(mJobConfig.getConfigType()).thenReturn(SYNC);
    when(mJobConfig.getSync()).thenReturn(jobSyncConfig);

    final Job mJob = Mockito.mock(Job.class);
    when(mJob.getScope()).thenReturn(CONNECTION_ID.toString());
    when(mJob.getConfig()).thenReturn(mJobConfig);
    when(mJob.getLastFailedAttempt()).thenReturn(Optional.of(mAttempt));
    when(mJob.getConfigType()).thenReturn(SYNC);

    when(jobPersistence.getJob(JOB_ID))
        .thenReturn(mJob);

    jobsHandler.jobFailure(new JobFailureRequest().jobId(JOB_ID).attemptNumber(1).connectionId(CONNECTION_ID).reason(failureReason));

    final SyncJobReportingContext expectedReportingContext = new SyncJobReportingContext(
        JOB_ID,
        jobSyncConfig.getSourceDefinitionVersionId(),
        jobSyncConfig.getDestinationDefinitionVersionId());

    final AttemptConfigReportingContext expectedAttemptConfig =
        new AttemptConfigReportingContext(
            mSyncConfig.getSourceConfiguration(),
            mSyncConfig.getDestinationConfiguration(),
            mSyncConfig.getState());

    verify(jobPersistence).failJob(JOB_ID);
    verify(jobNotifier).failJob(Mockito.any(), any());
    verify(jobErrorReporter).reportSyncJobFailure(CONNECTION_ID, failureSummary, expectedReportingContext, expectedAttemptConfig);
    verify(connectionTimelineEventService).writeEvent(eq(CONNECTION_ID), any());
  }

  @Test
  void setJobFailureWithNullJobSyncConfig() throws IOException {
    final AttemptFailureSummary failureSummary = new AttemptFailureSummary()
        .withFailures(Collections.singletonList(
            new FailureReason()
                .withFailureOrigin(FailureOrigin.SOURCE)));
    final String failureReason = "reason";

    final Attempt mAttempt = Mockito.mock(Attempt.class);
    Mockito.when(mAttempt.getFailureSummary()).thenReturn(Optional.of(failureSummary));

    final JobConfig mJobConfig = Mockito.mock(JobConfig.class);
    Mockito.when(mJobConfig.getSync()).thenReturn(null);

    final Job mJob = Mockito.mock(Job.class);
    Mockito.when(mJob.getScope()).thenReturn(CONNECTION_ID.toString());
    Mockito.when(mJob.getConfig()).thenReturn(mJobConfig);
    Mockito.when(mJob.getLastFailedAttempt()).thenReturn(Optional.of(mAttempt));
    Mockito.when(mJob.getConfigType()).thenReturn(SYNC);

    Mockito.when(jobPersistence.getJob(JOB_ID))
        .thenReturn(mJob);

    jobsHandler.jobFailure(new JobFailureRequest().jobId(JOB_ID).attemptNumber(1).connectionId(CONNECTION_ID).reason(failureReason));

    verify(jobPersistence).failJob(JOB_ID);
    verify(jobNotifier).failJob(Mockito.any(), any());
    verify(jobErrorReporter).reportSyncJobFailure(eq(CONNECTION_ID), eq(failureSummary), Mockito.any(), Mockito.any());
  }

  @Test
  void testCancelledJobsDoNotNotify() throws IOException {

    final AttemptFailureSummary failureSummary = new AttemptFailureSummary()
        .withFailures(Collections.singletonList(
            new FailureReason()
                .withFailureOrigin(FailureOrigin.SOURCE)));

    final Attempt mAttempt = Mockito.mock(Attempt.class);
    Mockito.when(mAttempt.getFailureSummary()).thenReturn(Optional.of(failureSummary));

    final JobConfig mJobConfig = Mockito.mock(JobConfig.class);
    Mockito.when(mJobConfig.getSync()).thenReturn(null);

    final Job mJob = Mockito.mock(Job.class);
    Mockito.when(mJob.getScope()).thenReturn(CONNECTION_ID.toString());
    Mockito.when(mJob.getConfig()).thenReturn(mJobConfig);
    Mockito.when(mJob.getLastFailedAttempt()).thenReturn(Optional.of(mAttempt));
    Mockito.when(mJob.getConfigType()).thenReturn(SYNC);
    Mockito.when(jobPersistence.getJob(JOB_ID)).thenReturn(mJob);

    jobsHandler.persistJobCancellation(CONNECTION_ID, JOB_ID, ATTEMPT_NUMBER, failureSummary);
    verify(jobNotifier, never()).failJob(Mockito.any(), any());
  }

}
