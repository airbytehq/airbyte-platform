/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.JobSuccessWithAttemptNumberRequest;
import io.airbyte.commons.server.JobStatus;
import io.airbyte.commons.server.handlers.helpers.JobCreationAndStatusUpdateHelper;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.config.JobOutput;
import io.airbyte.config.NormalizationSummary;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.StandardSyncSummary;
import io.airbyte.config.StandardSyncSummary.ReplicationStatus;
import io.airbyte.persistence.job.JobNotifier;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.models.Job;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * JobsHandlerTest.
 */
class JobsHandlerTest {

  private JobPersistence jobPersistence;
  private JobNotifier jobNotifier;
  private JobsHandler jobsHandler;
  private JobCreationAndStatusUpdateHelper helper;

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

  @BeforeEach
  void beforeEach() {
    jobPersistence = mock(JobPersistence.class);
    jobNotifier = mock(JobNotifier.class);
    helper = mock(JobCreationAndStatusUpdateHelper.class);

    jobsHandler = new JobsHandler(jobPersistence, helper, jobNotifier);
  }

  @Test
  void testJobSuccessWithAttemptNumber() throws IOException {
    final var request = new JobSuccessWithAttemptNumberRequest()
        .attemptNumber(ATTEMPT_NUMBER)
        .jobId(JOB_ID)
        .connectionId(UUID.randomUUID())
        .standardSyncOutput(standardSyncOutput);
    jobsHandler.jobSuccessWithAttemptNumber(request);

    verify(jobPersistence).writeOutput(JOB_ID, ATTEMPT_NUMBER, jobOutput);
    verify(jobPersistence).succeedAttempt(JOB_ID, ATTEMPT_NUMBER);
    verify(jobNotifier).successJob(any());
    verify(helper).trackCompletion(any(), eq(JobStatus.SUCCEEDED));
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
        .isInstanceOf(RetryableException.class)
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

}
