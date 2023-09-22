/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.airbyte.api.model.generated.JobSuccessWithAttemptNumberRequest;
import io.airbyte.commons.server.handlers.helpers.JobCreationAndStatusUpdateHelper;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.config.JobOutput;
import io.airbyte.config.NormalizationSummary;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.StandardSyncSummary;
import io.airbyte.config.StandardSyncSummary.ReplicationStatus;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.persistence.job.JobNotifier;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.tracker.JobTracker;
import io.airbyte.persistence.job.tracker.JobTracker.JobState;
import java.io.IOException;
import java.util.UUID;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class JobsHandlerTest {

  private JobPersistence jobPersistence;
  private JobTracker jobTracker;
  private JobNotifier jobNotifier;
  private JobsHandler jobsHandler;

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
    jobTracker = mock(JobTracker.class);
    jobNotifier = mock(JobNotifier.class);
    jobsHandler = new JobsHandler(jobPersistence,
        new JobCreationAndStatusUpdateHelper(jobPersistence, mock(ConfigRepository.class),
            jobNotifier,
            jobTracker),
        jobNotifier);
  }

  @Test
  void testJobSuccessWithAttemptNumber() throws IOException {
    var request = new JobSuccessWithAttemptNumberRequest()
        .attemptNumber(ATTEMPT_NUMBER)
        .jobId(JOB_ID)
        .connectionId(UUID.randomUUID())
        .standardSyncOutput(standardSyncOutput);
    jobsHandler.jobSuccessWithAttemptNumber(request);

    verify(jobPersistence).writeOutput(JOB_ID, ATTEMPT_NUMBER, jobOutput);
    verify(jobPersistence).succeedAttempt(JOB_ID, ATTEMPT_NUMBER);
    verify(jobNotifier).successJob(any());
    verify(jobTracker).trackSync(any(), eq(JobState.SUCCEEDED));
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

    verify(jobTracker, times(1)).trackSyncForInternalFailure(JOB_ID, CONNECTION_ID, ATTEMPT_NUMBER, JobState.SUCCEEDED, exception);
  }

}
