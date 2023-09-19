/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static io.airbyte.config.JobConfig.ConfigType.RESET_CONNECTION;
import static io.airbyte.config.JobConfig.ConfigType.SYNC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.airbyte.api.client.generated.AttemptApi;
import io.airbyte.api.client.generated.JobsApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.CreateNewAttemptNumberRequest;
import io.airbyte.api.client.model.generated.CreateNewAttemptNumberResponse;
import io.airbyte.api.client.model.generated.JobCreate;
import io.airbyte.api.client.model.generated.JobInfoRead;
import io.airbyte.api.client.model.generated.JobRead;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.AttemptFailureSummary;
import io.airbyte.config.FailureReason;
import io.airbyte.config.FailureReason.FailureOrigin;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.JobOutput;
import io.airbyte.config.JobResetConnectionConfig;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.NormalizationSummary;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.StandardSyncSummary;
import io.airbyte.config.StandardSyncSummary.ReplicationStatus;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.persistence.job.JobNotifier;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.errorreporter.JobErrorReporter;
import io.airbyte.persistence.job.errorreporter.SyncJobReportingContext;
import io.airbyte.persistence.job.models.Attempt;
import io.airbyte.persistence.job.models.AttemptStatus;
import io.airbyte.persistence.job.models.Job;
import io.airbyte.persistence.job.models.JobStatus;
import io.airbyte.persistence.job.tracker.JobTracker;
import io.airbyte.persistence.job.tracker.JobTracker.JobState;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.AttemptCreationInput;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.AttemptNumberCreationOutput;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.EnsureCleanJobStateInput;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobCreationInput;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobCreationOutput;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobFailureInput;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class JobCreationAndStatusUpdateActivityTest {

  public static final String REASON = "reason";

  @Mock
  private JobPersistence mJobPersistence;

  @Mock
  private JobNotifier mJobNotifier;

  @Mock
  private JobTracker mJobtracker;

  @Mock
  private JobErrorReporter mJobErrorReporter;

  @Mock
  private ConfigRepository mConfigRepository;

  @Mock
  private JobsApi jobsApi;

  @Mock
  private AttemptApi attemptApi;

  private JobCreationAndStatusUpdateActivityImpl jobCreationAndStatusUpdateActivity;

  private static final UUID CONNECTION_ID = UUID.randomUUID();
  private static final long JOB_ID = 123L;
  private static final long PREVIOUS_JOB_ID = 120L;
  private static final int ATTEMPT_NUMBER = 0;
  private static final int ATTEMPT_NUMBER_1 = 1;

  private static final String TEST_EXCEPTION_MESSAGE = "test";

  private static final StandardSyncOutput standardSyncOutput = new StandardSyncOutput()
      .withStandardSyncSummary(
          new StandardSyncSummary()
              .withStatus(ReplicationStatus.COMPLETED))
      .withNormalizationSummary(
          new NormalizationSummary());

  private static final JobOutput jobOutput = new JobOutput().withSync(standardSyncOutput);

  private static final AttemptFailureSummary failureSummary = new AttemptFailureSummary()
      .withFailures(Collections.singletonList(
          new FailureReason()
              .withFailureOrigin(FailureOrigin.SOURCE)));

  @BeforeEach
  void beforeEach() {
    jobCreationAndStatusUpdateActivity = new JobCreationAndStatusUpdateActivityImpl(
        mJobPersistence, mJobNotifier, mJobtracker, mConfigRepository, mJobErrorReporter, jobsApi,
        attemptApi);
  }

  @Nested
  class Creation {

    @Test
    @DisplayName("Test job creation")
    void createJob() throws ApiException {
      Mockito.when(jobsApi.createJob(new JobCreate().connectionId(CONNECTION_ID))).thenReturn(new JobInfoRead().job(new JobRead().id(JOB_ID)));
      final JobCreationOutput newJob = jobCreationAndStatusUpdateActivity.createNewJob(new JobCreationInput(CONNECTION_ID));

      assertEquals(JOB_ID, newJob.getJobId());
    }

    @Test
    @DisplayName("Test job creation throws retryable exception")
    void createJobThrows() throws ApiException {
      Mockito.when(jobsApi.createJob(Mockito.any())).thenThrow(new ApiException());
      assertThrows(RetryableException.class, () -> jobCreationAndStatusUpdateActivity.createNewJob(new JobCreationInput(CONNECTION_ID)));
    }

    @Test
    void isLastJobOrAttemptFailureTrueTest() throws Exception {
      final int activeAttemptNumber = 0;
      final Attempt activeAttempt = new Attempt(activeAttemptNumber, 1, Path.of(""), null, null, AttemptStatus.RUNNING, null, null, 4L, 5L, null);

      final Job previousJob = new Job(PREVIOUS_JOB_ID, ConfigType.SYNC, CONNECTION_ID.toString(),
          new JobConfig(), List.of(), JobStatus.SUCCEEDED, 4L, 4L, 5L);
      final Job activeJob = new Job(JOB_ID, ConfigType.SYNC, CONNECTION_ID.toString(), new JobConfig(), List.of(activeAttempt),
          JobStatus.RUNNING, 2L, 2L, 3L);

      final Set<ConfigType> configTypes = new HashSet<>();
      configTypes.add(SYNC);

      Mockito.when(mJobPersistence.listJobsIncludingId(configTypes, CONNECTION_ID.toString(), JOB_ID, 2))
          .thenReturn(List.of(activeJob, previousJob));
      final boolean result = jobCreationAndStatusUpdateActivity
          .isLastJobOrAttemptFailure(new JobCreationAndStatusUpdateActivity.JobCheckFailureInput(JOB_ID, 0, CONNECTION_ID));
      Assertions.assertThat(result).isEqualTo(false);
    }

    @Test
    void isLastJobOrAttemptFailureFalseTest() throws Exception {
      final int activeAttemptNumber = 0;
      final Attempt activeAttempt = new Attempt(activeAttemptNumber, 1, Path.of(""), null, null, AttemptStatus.RUNNING, null, null, 4L, 5L, null);

      final Job previousJob = new Job(PREVIOUS_JOB_ID, ConfigType.SYNC, CONNECTION_ID.toString(),
          new JobConfig(), List.of(), JobStatus.FAILED, 4L, 4L, 5L);
      final Job activeJob = new Job(JOB_ID, ConfigType.SYNC, CONNECTION_ID.toString(), new JobConfig(), List.of(activeAttempt),
          JobStatus.RUNNING, 2L, 2L, 3L);

      final Set<ConfigType> configTypes = new HashSet<>();
      configTypes.add(SYNC);

      Mockito.when(mJobPersistence.listJobsIncludingId(configTypes, CONNECTION_ID.toString(), JOB_ID, 2))
          .thenReturn(List.of(activeJob, previousJob));
      final boolean result = jobCreationAndStatusUpdateActivity
          .isLastJobOrAttemptFailure(new JobCreationAndStatusUpdateActivity.JobCheckFailureInput(JOB_ID, 0, CONNECTION_ID));
      Assertions.assertThat(result).isEqualTo(true);
    }

    @Test
    void isLastJobOrAttemptFailurePreviousAttemptFailureTest() throws Exception {
      final Attempt previousAttempt = new Attempt(0, 1, Path.of(""), null, null, AttemptStatus.FAILED, null, null, 2L, 3L, 3L);
      final int activeAttemptNumber = 1;
      final Attempt activeAttempt = new Attempt(activeAttemptNumber, 1, Path.of(""), null, null, AttemptStatus.RUNNING, null, null, 4L, 5L, null);

      final Job previousJob = new Job(PREVIOUS_JOB_ID, ConfigType.SYNC, CONNECTION_ID.toString(), new JobConfig(), List.of(),
          JobStatus.SUCCEEDED, 4L, 4L, 5L);
      final Job activeJob = new Job(JOB_ID, ConfigType.SYNC, CONNECTION_ID.toString(), new JobConfig(), List.of(activeAttempt, previousAttempt),
          JobStatus.RUNNING, 2L, 2L, 3L);

      final Set<ConfigType> configTypes = new HashSet<>();
      configTypes.add(SYNC);

      Mockito.when(mJobPersistence.listJobsIncludingId(configTypes, CONNECTION_ID.toString(), JOB_ID, 2))
          .thenReturn(List.of(activeJob, previousJob));
      final boolean result = jobCreationAndStatusUpdateActivity
          .isLastJobOrAttemptFailure(new JobCreationAndStatusUpdateActivity.JobCheckFailureInput(JOB_ID, 1, CONNECTION_ID));
      Assertions.assertThat(result).isEqualTo(true);
    }

    @Test
    @DisplayName("Test attempt creation")
    void createAttemptNumber() throws ApiException {
      Mockito.when(attemptApi.createNewAttemptNumber(new CreateNewAttemptNumberRequest().jobId(JOB_ID)))
          .thenReturn(new CreateNewAttemptNumberResponse().attemptNumber(ATTEMPT_NUMBER_1));

      final AttemptNumberCreationOutput output = jobCreationAndStatusUpdateActivity.createNewAttemptNumber(new AttemptCreationInput(JOB_ID));
      Assertions.assertThat(output.getAttemptNumber()).isEqualTo(ATTEMPT_NUMBER_1);
    }

    @Test
    @DisplayName("Test exception errors are properly wrapped")
    void createAttemptNumberThrowException() throws ApiException {
      Mockito.when(attemptApi.createNewAttemptNumber(new CreateNewAttemptNumberRequest().jobId(JOB_ID)))
          .thenThrow(new ApiException());

      Assertions.assertThatThrownBy(() -> jobCreationAndStatusUpdateActivity.createNewAttemptNumber(new AttemptCreationInput(
          JOB_ID)))
          .isInstanceOf(RetryableException.class)
          .hasCauseInstanceOf(ApiException.class);
    }

  }

  @Nested
  class Update {

    @Test
    void setJobSuccess() throws IOException {
      jobCreationAndStatusUpdateActivity.jobSuccessWithAttemptNumber(
          new JobCreationAndStatusUpdateActivity.JobSuccessInputWithAttemptNumber(JOB_ID, ATTEMPT_NUMBER, CONNECTION_ID, standardSyncOutput));

      verify(mJobPersistence).writeOutput(JOB_ID, ATTEMPT_NUMBER, jobOutput);
      verify(mJobPersistence).succeedAttempt(JOB_ID, ATTEMPT_NUMBER);
      verify(mJobNotifier).successJob(Mockito.any());
      verify(mJobtracker).trackSync(Mockito.any(), eq(JobState.SUCCEEDED));
    }

    @Test
    void setJobSuccessWrapException() throws IOException {
      final IOException exception = new IOException(TEST_EXCEPTION_MESSAGE);
      Mockito.doThrow(exception)
          .when(mJobPersistence).succeedAttempt(JOB_ID, ATTEMPT_NUMBER);

      Assertions
          .assertThatThrownBy(() -> jobCreationAndStatusUpdateActivity.jobSuccessWithAttemptNumber(
              new JobCreationAndStatusUpdateActivity.JobSuccessInputWithAttemptNumber(JOB_ID, ATTEMPT_NUMBER, CONNECTION_ID, null)))
          .isInstanceOf(RetryableException.class)
          .hasCauseInstanceOf(IOException.class);

      verify(mJobtracker, times(1)).trackSyncForInternalFailure(JOB_ID, CONNECTION_ID, ATTEMPT_NUMBER, JobState.SUCCEEDED, exception);
    }

    @Test
    void setJobFailure() throws IOException {
      final Attempt mAttempt = Mockito.mock(Attempt.class);
      Mockito.when(mAttempt.getFailureSummary()).thenReturn(Optional.of(failureSummary));

      final JobSyncConfig jobSyncConfig = new JobSyncConfig()
          .withSourceDefinitionVersionId(UUID.randomUUID())
          .withDestinationDefinitionVersionId(UUID.randomUUID());

      final JobConfig mJobConfig = Mockito.mock(JobConfig.class);
      Mockito.when(mJobConfig.getConfigType()).thenReturn(SYNC);
      Mockito.when(mJobConfig.getSync()).thenReturn(jobSyncConfig);

      final Job mJob = Mockito.mock(Job.class);
      Mockito.when(mJob.getScope()).thenReturn(CONNECTION_ID.toString());
      Mockito.when(mJob.getConfig()).thenReturn(mJobConfig);
      Mockito.when(mJob.getLastFailedAttempt()).thenReturn(Optional.of(mAttempt));

      Mockito.when(mJobPersistence.getJob(JOB_ID))
          .thenReturn(mJob);

      jobCreationAndStatusUpdateActivity.jobFailure(new JobFailureInput(JOB_ID, 1, CONNECTION_ID, REASON));

      final SyncJobReportingContext expectedReportingContext = new SyncJobReportingContext(
          JOB_ID,
          jobSyncConfig.getSourceDefinitionVersionId(),
          jobSyncConfig.getDestinationDefinitionVersionId());

      verify(mJobPersistence).failJob(JOB_ID);
      verify(mJobNotifier).failJob(eq(REASON), Mockito.any());
      verify(mJobErrorReporter).reportSyncJobFailure(CONNECTION_ID, failureSummary, expectedReportingContext);
    }

    @Test
    void setJobFailureWrapException() throws IOException {
      final Exception exception = new IOException(TEST_EXCEPTION_MESSAGE);
      Mockito.doThrow(exception)
          .when(mJobPersistence).failJob(JOB_ID);

      Assertions
          .assertThatThrownBy(() -> jobCreationAndStatusUpdateActivity.jobFailure(new JobFailureInput(JOB_ID, ATTEMPT_NUMBER_1, CONNECTION_ID, "")))
          .isInstanceOf(RetryableException.class)
          .hasCauseInstanceOf(IOException.class);

      verify(mJobtracker, times(1)).trackSyncForInternalFailure(JOB_ID, CONNECTION_ID, ATTEMPT_NUMBER_1, JobState.FAILED, exception);
    }

    @Test
    void setJobFailureWithNullJobSyncConfig() throws IOException {
      final Attempt mAttempt = Mockito.mock(Attempt.class);
      Mockito.when(mAttempt.getFailureSummary()).thenReturn(Optional.of(failureSummary));

      final JobConfig mJobConfig = Mockito.mock(JobConfig.class);
      Mockito.when(mJobConfig.getSync()).thenReturn(null);

      final Job mJob = Mockito.mock(Job.class);
      Mockito.when(mJob.getScope()).thenReturn(CONNECTION_ID.toString());
      Mockito.when(mJob.getConfig()).thenReturn(mJobConfig);
      Mockito.when(mJob.getLastFailedAttempt()).thenReturn(Optional.of(mAttempt));

      Mockito.when(mJobPersistence.getJob(JOB_ID))
          .thenReturn(mJob);

      jobCreationAndStatusUpdateActivity.jobFailure(new JobFailureInput(JOB_ID, 1, CONNECTION_ID, REASON));

      verify(mJobPersistence).failJob(JOB_ID);
      verify(mJobNotifier).failJob(eq(REASON), Mockito.any());
      verify(mJobErrorReporter).reportSyncJobFailure(eq(CONNECTION_ID), eq(failureSummary), Mockito.any());
    }

    @Test
    void setAttemptFailure() throws IOException {
      jobCreationAndStatusUpdateActivity
          .attemptFailureWithAttemptNumber(new JobCreationAndStatusUpdateActivity.AttemptNumberFailureInput(JOB_ID, ATTEMPT_NUMBER, CONNECTION_ID,
              standardSyncOutput, failureSummary));

      verify(mJobPersistence).failAttempt(JOB_ID, ATTEMPT_NUMBER);
      verify(mJobPersistence).writeOutput(JOB_ID, ATTEMPT_NUMBER, jobOutput);
      verify(mJobPersistence).writeAttemptFailureSummary(JOB_ID, ATTEMPT_NUMBER, failureSummary);
    }

    @Test
    void setAttemptFailureManuallyTerminated() throws IOException {
      jobCreationAndStatusUpdateActivity
          .attemptFailureWithAttemptNumber(
              new JobCreationAndStatusUpdateActivity.AttemptNumberFailureInput(JOB_ID, ATTEMPT_NUMBER, CONNECTION_ID, standardSyncOutput, null));

      verify(mJobPersistence).failAttempt(JOB_ID, ATTEMPT_NUMBER);
      verify(mJobPersistence).writeOutput(JOB_ID, ATTEMPT_NUMBER, jobOutput);
      verify(mJobPersistence).writeAttemptFailureSummary(JOB_ID, ATTEMPT_NUMBER, null);
    }

    @Test
    void setAttemptFailureWrapException() throws IOException {
      final Exception exception = new IOException(TEST_EXCEPTION_MESSAGE);
      Mockito.doThrow(exception)
          .when(mJobPersistence).failAttempt(JOB_ID, ATTEMPT_NUMBER);

      Assertions
          .assertThatThrownBy(
              () -> jobCreationAndStatusUpdateActivity
                  .attemptFailureWithAttemptNumber(
                      new JobCreationAndStatusUpdateActivity.AttemptNumberFailureInput(JOB_ID, ATTEMPT_NUMBER, CONNECTION_ID, null, failureSummary)))
          .isInstanceOf(RetryableException.class)
          .hasCauseInstanceOf(IOException.class);
    }

    @Test
    void setJobCancelled() throws IOException {
      jobCreationAndStatusUpdateActivity.jobCancelledWithAttemptNumber(
          new JobCreationAndStatusUpdateActivity.JobCancelledInputWithAttemptNumber(JOB_ID, ATTEMPT_NUMBER, CONNECTION_ID, failureSummary));

      // attempt must be failed before job is cancelled, or else job state machine is not respected
      final InOrder orderVerifier = Mockito.inOrder(mJobPersistence);
      orderVerifier.verify(mJobPersistence).failAttempt(JOB_ID, ATTEMPT_NUMBER);
      orderVerifier.verify(mJobPersistence).writeAttemptFailureSummary(JOB_ID, ATTEMPT_NUMBER, failureSummary);
      orderVerifier.verify(mJobPersistence).cancelJob(JOB_ID);
    }

    @Test
    void setJobCancelledWrapException() throws IOException {
      final Exception exception = new IOException();
      Mockito.doThrow(exception)
          .when(mJobPersistence).cancelJob(JOB_ID);

      Assertions
          .assertThatThrownBy(() -> jobCreationAndStatusUpdateActivity.jobCancelledWithAttemptNumber(
              new JobCreationAndStatusUpdateActivity.JobCancelledInputWithAttemptNumber(JOB_ID, ATTEMPT_NUMBER, CONNECTION_ID, null)))
          .isInstanceOf(RetryableException.class)
          .hasCauseInstanceOf(IOException.class);

      verify(mJobtracker, times(1)).trackSyncForInternalFailure(JOB_ID, CONNECTION_ID, ATTEMPT_NUMBER, JobState.FAILED, exception);
    }

    @Test
    void ensureCleanJobState() throws IOException {
      final JobConfig jobConfig = new JobConfig()
          .withConfigType(SYNC)
          .withSync(new JobSyncConfig()
              .withSourceDefinitionVersionId(UUID.randomUUID())
              .withDestinationDefinitionVersionId(UUID.randomUUID()));
      final Attempt failedAttempt = new Attempt(0, 1, Path.of(""), null, null, AttemptStatus.FAILED, null, null, 2L, 3L, 3L);
      final int runningAttemptNumber = 1;
      final Attempt runningAttempt = new Attempt(runningAttemptNumber, 1, Path.of(""), null, null, AttemptStatus.RUNNING, null, null, 4L, 5L, null);
      final Job runningJob = new Job(1, ConfigType.SYNC, CONNECTION_ID.toString(), jobConfig, List.of(failedAttempt, runningAttempt),
          JobStatus.RUNNING, 2L, 2L, 3L);

      final Job pendingJob = new Job(2, ConfigType.SYNC, CONNECTION_ID.toString(), jobConfig, List.of(), JobStatus.PENDING, 4L, 4L, 5L);

      Mockito.when(mJobPersistence.listJobsForConnectionWithStatuses(CONNECTION_ID, Job.REPLICATION_TYPES, JobStatus.NON_TERMINAL_STATUSES))
          .thenReturn(List.of(runningJob, pendingJob));
      Mockito.when(mJobPersistence.getJob(runningJob.getId())).thenReturn(runningJob);
      Mockito.when(mJobPersistence.getJob(pendingJob.getId())).thenReturn(pendingJob);

      jobCreationAndStatusUpdateActivity.ensureCleanJobState(new EnsureCleanJobStateInput(CONNECTION_ID));

      verify(mJobPersistence).failJob(runningJob.getId());
      verify(mJobPersistence).failJob(pendingJob.getId());
      verify(mJobPersistence).failAttempt(runningJob.getId(), runningAttemptNumber);
      verify(mJobPersistence).writeAttemptFailureSummary(eq(runningJob.getId()), eq(runningAttemptNumber), any());
      verify(mJobPersistence).getJob(runningJob.getId());
      verify(mJobPersistence).getJob(pendingJob.getId());
      verify(mJobNotifier).failJob(any(), eq(runningJob));
      verify(mJobNotifier).failJob(any(), eq(pendingJob));
      verify(mJobtracker).trackSync(runningJob, JobState.FAILED);
      verify(mJobtracker).trackSync(pendingJob, JobState.FAILED);
      Mockito.verifyNoMoreInteractions(mJobPersistence, mJobNotifier, mJobtracker);
    }

  }

  @Test
  void testReleaseStageOrdering() {
    final List<ReleaseStage> input = List.of(ReleaseStage.ALPHA, ReleaseStage.CUSTOM, ReleaseStage.BETA, ReleaseStage.GENERALLY_AVAILABLE);
    final List<ReleaseStage> expected = List.of(ReleaseStage.CUSTOM, ReleaseStage.ALPHA, ReleaseStage.BETA, ReleaseStage.GENERALLY_AVAILABLE);

    Assertions.assertThat(JobCreationAndStatusUpdateActivityImpl.orderByReleaseStageAsc(input))
        .containsExactlyElementsOf(expected);
  }

  @Test
  void testGetSyncJobToReleaseStages() throws IOException {
    final UUID sourceDefVersionId = UUID.randomUUID();
    final UUID destinationDefVersionId = UUID.randomUUID();
    final JobConfig jobConfig = new JobConfig()
        .withConfigType(SYNC)
        .withSync(new JobSyncConfig()
            .withSourceDefinitionVersionId(sourceDefVersionId)
            .withDestinationDefinitionVersionId(destinationDefVersionId));
    final Job job = new Job(JOB_ID, ConfigType.SYNC, CONNECTION_ID.toString(), jobConfig, List.of(), JobStatus.PENDING, 0L, 0L, 0L);

    Mockito.when(mConfigRepository.getActorDefinitionVersions(List.of(destinationDefVersionId, sourceDefVersionId)))
        .thenReturn(List.of(
            new ActorDefinitionVersion().withReleaseStage(ReleaseStage.ALPHA),
            new ActorDefinitionVersion().withReleaseStage(ReleaseStage.GENERALLY_AVAILABLE)));

    final List<ReleaseStage> releaseStages = jobCreationAndStatusUpdateActivity.getJobToReleaseStages(job);

    Assertions.assertThat(releaseStages).contains(ReleaseStage.ALPHA, ReleaseStage.GENERALLY_AVAILABLE);
  }

  @Test
  void testGetResetJobToReleaseStages() throws IOException {
    final UUID destinationDefVersionId = UUID.randomUUID();
    final JobConfig jobConfig = new JobConfig()
        .withConfigType(RESET_CONNECTION)
        .withResetConnection(new JobResetConnectionConfig()
            .withDestinationDefinitionVersionId(destinationDefVersionId));
    final Job job = new Job(JOB_ID, RESET_CONNECTION, CONNECTION_ID.toString(), jobConfig, List.of(), JobStatus.PENDING, 0L, 0L, 0L);

    Mockito.when(mConfigRepository.getActorDefinitionVersions(List.of(destinationDefVersionId)))
        .thenReturn(List.of(
            new ActorDefinitionVersion().withReleaseStage(ReleaseStage.ALPHA)));
    final List<ReleaseStage> releaseStages = jobCreationAndStatusUpdateActivity.getJobToReleaseStages(job);

    Assertions.assertThat(releaseStages).contains(ReleaseStage.ALPHA);
  }

}
