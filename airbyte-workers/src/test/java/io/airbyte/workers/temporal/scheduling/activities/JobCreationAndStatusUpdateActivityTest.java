/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static io.airbyte.config.JobConfig.ConfigType.RESET_CONNECTION;
import static io.airbyte.config.JobConfig.ConfigType.SYNC;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.airbyte.api.client.generated.AttemptApi;
import io.airbyte.api.client.generated.JobsApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.BooleanRead;
import io.airbyte.api.client.model.generated.CreateNewAttemptNumberRequest;
import io.airbyte.api.client.model.generated.CreateNewAttemptNumberResponse;
import io.airbyte.api.client.model.generated.JobCreate;
import io.airbyte.api.client.model.generated.JobInfoRead;
import io.airbyte.api.client.model.generated.JobRead;
import io.airbyte.api.client.model.generated.JobSuccessWithAttemptNumberRequest;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.AttemptFailureSummary;
import io.airbyte.config.FailureReason;
import io.airbyte.config.FailureReason.FailureOrigin;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.JobResetConnectionConfig;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.NormalizationSummary;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.StandardSyncSummary;
import io.airbyte.config.StandardSyncSummary.ReplicationStatus;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.featureflag.UseNewIsLastJobOrAttemptFailure;
import io.airbyte.persistence.job.JobNotifier;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.errorreporter.JobErrorReporter;
import io.airbyte.persistence.job.errorreporter.SyncJobReportingContext;
import io.airbyte.persistence.job.models.Attempt;
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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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

  private FeatureFlagClient mFeatureFlagClient;

  private JobCreationAndStatusUpdateActivityImpl jobCreationAndStatusUpdateActivity;

  private static final UUID CONNECTION_ID = UUID.randomUUID();
  private static final long JOB_ID = 123L;
  private static final int ATTEMPT_NUMBER = 0;
  private static final int ATTEMPT_NUMBER_1 = 1;

  private static final String TEST_EXCEPTION_MESSAGE = "test";

  private static final StandardSyncOutput standardSyncOutput = new StandardSyncOutput()
      .withStandardSyncSummary(
          new StandardSyncSummary()
              .withStatus(ReplicationStatus.COMPLETED))
      .withNormalizationSummary(
          new NormalizationSummary());

  private static final AttemptFailureSummary failureSummary = new AttemptFailureSummary()
      .withFailures(Collections.singletonList(
          new FailureReason()
              .withFailureOrigin(FailureOrigin.SOURCE)));

  @BeforeEach
  void beforeEach() {
    mFeatureFlagClient = Mockito.mock(TestClient.class);

    jobCreationAndStatusUpdateActivity = new JobCreationAndStatusUpdateActivityImpl(
        mJobPersistence, mJobNotifier, mJobtracker, mConfigRepository, mJobErrorReporter, jobsApi,
        attemptApi, mFeatureFlagClient);
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

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 5, 20, 30, 1439, 11})
    void isLastJobOrAttemptFailureReturnsTrueIfNotFirstAttemptForJob(final int attemptNumber) {
      Mockito.when(mFeatureFlagClient.boolVariation(eq(UseNewIsLastJobOrAttemptFailure.INSTANCE), any()))
          .thenReturn(true);

      final var input = new JobCreationAndStatusUpdateActivity.JobCheckFailureInput(
          JOB_ID,
          attemptNumber,
          CONNECTION_ID);
      final boolean result = jobCreationAndStatusUpdateActivity.isLastJobOrAttemptFailure(input);

      assertTrue(result);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void isLastJobOrAttemptFailureReturnsChecksPreviousJobIfFirstAttempt(final boolean didSucceed) throws ApiException {
      Mockito.when(mFeatureFlagClient.boolVariation(eq(UseNewIsLastJobOrAttemptFailure.INSTANCE), any()))
          .thenReturn(true);

      final var input = new JobCreationAndStatusUpdateActivity.JobCheckFailureInput(
          JOB_ID,
          0,
          CONNECTION_ID);

      Mockito.when(jobsApi.didPreviousJobSucceed(any()))
          .thenReturn(new BooleanRead().value(didSucceed));

      final boolean result = jobCreationAndStatusUpdateActivity.isLastJobOrAttemptFailure(input);

      assertEquals(!didSucceed, result);
    }

    @Test
    void isLastJobOrAttemptFailureThrowsRetryableErrorIfApiCallFails() throws ApiException {
      Mockito.when(mFeatureFlagClient.boolVariation(eq(UseNewIsLastJobOrAttemptFailure.INSTANCE), any()))
          .thenReturn(true);

      final var input = new JobCreationAndStatusUpdateActivity.JobCheckFailureInput(
          JOB_ID,
          0,
          CONNECTION_ID);

      Mockito.when(jobsApi.didPreviousJobSucceed(any()))
          .thenThrow(new ApiException("bang"));

      assertThrows(RetryableException.class, () -> jobCreationAndStatusUpdateActivity.isLastJobOrAttemptFailure(input));
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
    void setJobSuccess() throws ApiException {
      var request =
          new JobCreationAndStatusUpdateActivity.JobSuccessInputWithAttemptNumber(JOB_ID, ATTEMPT_NUMBER, CONNECTION_ID, standardSyncOutput);
      jobCreationAndStatusUpdateActivity.jobSuccessWithAttemptNumber(request);
      verify(jobsApi).jobSuccessWithAttemptNumber(new JobSuccessWithAttemptNumberRequest()
          .attemptNumber(request.getAttemptNumber())
          .jobId(request.getJobId())
          .connectionId(request.getConnectionId())
          .standardSyncOutput(request.getStandardSyncOutput()));
    }

    @Test
    void setJobSuccessWrapException() throws ApiException {
      final ApiException exception = new ApiException(TEST_EXCEPTION_MESSAGE);
      Mockito.doThrow(exception)
          .when(jobsApi).jobSuccessWithAttemptNumber(any());

      Assertions
          .assertThatThrownBy(() -> jobCreationAndStatusUpdateActivity.jobSuccessWithAttemptNumber(
              new JobCreationAndStatusUpdateActivity.JobSuccessInputWithAttemptNumber(JOB_ID, ATTEMPT_NUMBER, CONNECTION_ID, null)))
          .isInstanceOf(RetryableException.class)
          .hasCauseInstanceOf(ApiException.class);
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
    void attemptFailureWithAttemptNumberHappyPath() {
      final var input = new JobCreationAndStatusUpdateActivity.AttemptNumberFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
          standardSyncOutput,
          failureSummary);

      assertDoesNotThrow(
          () -> jobCreationAndStatusUpdateActivity.attemptFailureWithAttemptNumber(input));
    }

    @Test
    void attemptFailureWithAttemptNumberThrowsRetryableOnApiFailure() throws ApiException {
      final var input = new JobCreationAndStatusUpdateActivity.AttemptNumberFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
          standardSyncOutput,
          failureSummary);

      Mockito.doThrow(new ApiException("bang")).when(attemptApi).failAttempt(any());

      assertThrows(
          RetryableException.class,
          () -> jobCreationAndStatusUpdateActivity.attemptFailureWithAttemptNumber(input));
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
    void ensureCleanJobStateHappyPath() {
      assertDoesNotThrow(
          () -> jobCreationAndStatusUpdateActivity.ensureCleanJobState(new EnsureCleanJobStateInput(CONNECTION_ID)));
    }

    @Test
    void ensureCleanJobStateThrowsRetryableOnApiFailure() throws ApiException {
      Mockito.doThrow(new ApiException("bang")).when(jobsApi).failNonTerminalJobs(any());

      assertThrows(
          RetryableException.class,
          () -> jobCreationAndStatusUpdateActivity.ensureCleanJobState(new EnsureCleanJobStateInput(CONNECTION_ID)));
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
