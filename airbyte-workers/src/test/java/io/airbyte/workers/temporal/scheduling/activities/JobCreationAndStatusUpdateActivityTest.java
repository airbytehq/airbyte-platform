/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.AttemptApi;
import io.airbyte.api.client.generated.JobsApi;
import io.airbyte.api.client.model.generated.BooleanRead;
import io.airbyte.api.client.model.generated.CreateNewAttemptNumberRequest;
import io.airbyte.api.client.model.generated.CreateNewAttemptNumberResponse;
import io.airbyte.api.client.model.generated.JobConfigType;
import io.airbyte.api.client.model.generated.JobCreate;
import io.airbyte.api.client.model.generated.JobFailureRequest;
import io.airbyte.api.client.model.generated.JobInfoRead;
import io.airbyte.api.client.model.generated.JobRead;
import io.airbyte.api.client.model.generated.JobStatus;
import io.airbyte.api.client.model.generated.JobSuccessWithAttemptNumberRequest;
import io.airbyte.api.client.model.generated.PersistCancelJobRequestBody;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.config.AttemptFailureSummary;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.FailureReason;
import io.airbyte.config.FailureReason.FailureOrigin;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.StandardSyncSummary;
import io.airbyte.config.StandardSyncSummary.ReplicationStatus;
import io.airbyte.config.State;
import io.airbyte.featureflag.TestClient;
import io.airbyte.workers.storage.activities.OutputStorageClient;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.AttemptCreationInput;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.AttemptNumberCreationOutput;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.EnsureCleanJobStateInput;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobCreationInput;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobCreationOutput;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobFailureInput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("PMD")
class JobCreationAndStatusUpdateActivityTest {

  public static final String REASON = "reason";
  private static final String EXCEPTION_MESSAGE = "bang";

  @Mock
  private AirbyteApiClient airbyteApiClient;

  @Mock
  private JobsApi jobsApi;

  @Mock
  private AttemptApi attemptApi;

  @Mock
  private TestClient featureFlagClient;

  @Mock
  private OutputStorageClient<State> outputStateStorageClient;

  @Mock
  private OutputStorageClient<ConfiguredAirbyteCatalog> outputCatalogStorageClient;

  private JobCreationAndStatusUpdateActivityImpl jobCreationAndStatusUpdateActivity;

  private static final UUID CONNECTION_ID = UUID.randomUUID();
  private static final long JOB_ID = 123L;
  private static final int ATTEMPT_NUMBER = 0;
  private static final int ATTEMPT_NUMBER_1 = 1;

  private static final String TEST_EXCEPTION_MESSAGE = "test";

  private static final StandardSyncOutput standardSyncOutput = new StandardSyncOutput()
      .withStandardSyncSummary(
          new StandardSyncSummary()
              .withStatus(ReplicationStatus.COMPLETED));

  private static final AttemptFailureSummary failureSummary = new AttemptFailureSummary()
      .withFailures(Collections.singletonList(
          new FailureReason()
              .withFailureOrigin(FailureOrigin.SOURCE)));

  @BeforeEach
  void beforeEach() {
    jobCreationAndStatusUpdateActivity = new JobCreationAndStatusUpdateActivityImpl(
        airbyteApiClient,
        featureFlagClient,
        outputStateStorageClient,
        outputCatalogStorageClient);
  }

  @Nested
  class Creation {

    @Test
    @DisplayName("Test job creation")
    void createJob() throws IOException {
      when(airbyteApiClient.getJobsApi()).thenReturn(jobsApi);
      when(jobsApi.createJob(new JobCreate(CONNECTION_ID)))
          .thenReturn(new JobInfoRead(new JobRead(JOB_ID, JobConfigType.SYNC, CONNECTION_ID.toString(), System.currentTimeMillis(),
              System.currentTimeMillis(), JobStatus.SUCCEEDED, null, null, null, null, null, null), List.of()));
      final JobCreationOutput newJob = jobCreationAndStatusUpdateActivity.createNewJob(new JobCreationInput(CONNECTION_ID));

      assertEquals(JOB_ID, newJob.getJobId());
    }

    @Test
    @DisplayName("Test job creation throws retryable exception")
    void createJobThrows() throws IOException {
      when(airbyteApiClient.getJobsApi()).thenReturn(jobsApi);
      when(jobsApi.createJob(Mockito.any())).thenThrow(new IOException());
      assertThrows(RetryableException.class, () -> jobCreationAndStatusUpdateActivity.createNewJob(new JobCreationInput(CONNECTION_ID)));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 5, 20, 30, 1439, 11})
    void isLastJobOrAttemptFailureReturnsTrueIfNotFirstAttemptForJob(final int attemptNumber) {
      final var input = new JobCreationAndStatusUpdateActivity.JobCheckFailureInput(
          JOB_ID,
          attemptNumber,
          CONNECTION_ID);
      final boolean result = jobCreationAndStatusUpdateActivity.isLastJobOrAttemptFailure(input);

      assertTrue(result);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void isLastJobOrAttemptFailureReturnsChecksPreviousJobIfFirstAttempt(final boolean didSucceed) throws IOException {
      final var input = new JobCreationAndStatusUpdateActivity.JobCheckFailureInput(
          JOB_ID,
          0,
          CONNECTION_ID);

      when(airbyteApiClient.getJobsApi()).thenReturn(jobsApi);
      when(jobsApi.didPreviousJobSucceed(any()))
          .thenReturn(new BooleanRead(didSucceed));

      final boolean result = jobCreationAndStatusUpdateActivity.isLastJobOrAttemptFailure(input);

      assertEquals(!didSucceed, result);
    }

    @Test
    void isLastJobOrAttemptFailureThrowsRetryableErrorIfApiCallFails() throws IOException {
      final var input = new JobCreationAndStatusUpdateActivity.JobCheckFailureInput(
          JOB_ID,
          0,
          CONNECTION_ID);

      when(airbyteApiClient.getJobsApi()).thenReturn(jobsApi);
      when(jobsApi.didPreviousJobSucceed(any()))
          .thenThrow(new IOException(EXCEPTION_MESSAGE));

      assertThrows(RetryableException.class, () -> jobCreationAndStatusUpdateActivity.isLastJobOrAttemptFailure(input));
    }

    @Test
    @DisplayName("Test attempt creation")
    void createAttemptNumber() throws IOException {
      when(airbyteApiClient.getAttemptApi()).thenReturn(attemptApi);
      when(attemptApi.createNewAttemptNumber(new CreateNewAttemptNumberRequest(JOB_ID)))
          .thenReturn(new CreateNewAttemptNumberResponse(ATTEMPT_NUMBER_1));

      final AttemptNumberCreationOutput output = jobCreationAndStatusUpdateActivity.createNewAttemptNumber(new AttemptCreationInput(JOB_ID));
      Assertions.assertThat(output.getAttemptNumber()).isEqualTo(ATTEMPT_NUMBER_1);
    }

    @Test
    @DisplayName("Test exception errors are properly wrapped")
    void createAttemptNumberThrowException() throws IOException {
      when(airbyteApiClient.getAttemptApi()).thenReturn(attemptApi);
      when(attemptApi.createNewAttemptNumber(new CreateNewAttemptNumberRequest(JOB_ID)))
          .thenThrow(new IOException());

      Assertions.assertThatThrownBy(() -> jobCreationAndStatusUpdateActivity.createNewAttemptNumber(new AttemptCreationInput(
          JOB_ID)))
          .isInstanceOf(RetryableException.class)
          .hasCauseInstanceOf(IOException.class);
    }

  }

  @Nested
  class Update {

    @Test
    void setJobSuccess() throws IOException {
      when(airbyteApiClient.getJobsApi()).thenReturn(jobsApi);
      final var request =
          new JobCreationAndStatusUpdateActivity.JobSuccessInputWithAttemptNumber(JOB_ID, ATTEMPT_NUMBER, CONNECTION_ID, standardSyncOutput);
      jobCreationAndStatusUpdateActivity.jobSuccessWithAttemptNumber(request);
      verify(jobsApi).jobSuccessWithAttemptNumber(new JobSuccessWithAttemptNumberRequest(
          request.getJobId(),
          request.getAttemptNumber(),
          request.getConnectionId(),
          request.getStandardSyncOutput()));
    }

    @Test
    void setJobSuccessWrapException() throws IOException {
      when(airbyteApiClient.getJobsApi()).thenReturn(jobsApi);
      final IOException exception = new IOException(TEST_EXCEPTION_MESSAGE);
      Mockito.doThrow(exception)
          .when(jobsApi).jobSuccessWithAttemptNumber(any());

      Assertions
          .assertThatThrownBy(() -> jobCreationAndStatusUpdateActivity.jobSuccessWithAttemptNumber(
              new JobCreationAndStatusUpdateActivity.JobSuccessInputWithAttemptNumber(JOB_ID, ATTEMPT_NUMBER, CONNECTION_ID, standardSyncOutput)))
          .isInstanceOf(RetryableException.class)
          .hasCauseInstanceOf(IOException.class);
    }

    @Test
    void setJobFailure() throws IOException {
      when(airbyteApiClient.getJobsApi()).thenReturn(jobsApi);
      jobCreationAndStatusUpdateActivity.jobFailure(new JobFailureInput(JOB_ID, 1, CONNECTION_ID, REASON));
      verify(jobsApi).jobFailure(new JobFailureRequest(JOB_ID, 1, CONNECTION_ID, REASON));
    }

    @Test
    void setJobFailureWithNullJobSyncConfig() throws IOException {
      when(airbyteApiClient.getJobsApi()).thenReturn(jobsApi);
      when(jobsApi.jobFailure(any())).thenThrow(new IOException());

      Assertions
          .assertThatThrownBy(() -> jobCreationAndStatusUpdateActivity.jobFailure(new JobFailureInput(JOB_ID, 1, CONNECTION_ID, REASON)))
          .isInstanceOf(RetryableException.class)
          .hasCauseInstanceOf(IOException.class);
      verify(jobsApi).jobFailure(new JobFailureRequest(JOB_ID, 1, CONNECTION_ID, REASON));
    }

    @Test
    void attemptFailureWithAttemptNumberHappyPath() {
      when(airbyteApiClient.getAttemptApi()).thenReturn(attemptApi);
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
    void attemptFailureWithAttemptNumberThrowsRetryableOnApiFailure() throws IOException {
      when(airbyteApiClient.getAttemptApi()).thenReturn(attemptApi);
      final var input = new JobCreationAndStatusUpdateActivity.AttemptNumberFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
          standardSyncOutput,
          failureSummary);

      Mockito.doThrow(new IOException(EXCEPTION_MESSAGE)).when(attemptApi).failAttempt(any());

      assertThrows(
          RetryableException.class,
          () -> jobCreationAndStatusUpdateActivity.attemptFailureWithAttemptNumber(input));
    }

    @Test
    void cancelJobHappyPath() throws IOException {
      when(airbyteApiClient.getJobsApi()).thenReturn(jobsApi);
      final var input = new JobCreationAndStatusUpdateActivity.JobCancelledInputWithAttemptNumber(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
          failureSummary);

      final ArgumentCaptor<PersistCancelJobRequestBody> jobReq = ArgumentCaptor.forClass(PersistCancelJobRequestBody.class);

      jobCreationAndStatusUpdateActivity.jobCancelledWithAttemptNumber(input);

      verify(jobsApi).persistJobCancellation(jobReq.capture());
      assertEquals(JOB_ID, jobReq.getValue().getJobId());
      assertEquals(ATTEMPT_NUMBER, jobReq.getValue().getAttemptNumber());
      assertEquals(CONNECTION_ID, jobReq.getValue().getConnectionId());
      assertEquals(failureSummary, jobReq.getValue().getAttemptFailureSummary());
    }

    @Test
    void cancelJobThrowsRetryableOnJobsApiFailure() throws IOException {
      when(airbyteApiClient.getJobsApi()).thenReturn(jobsApi);
      final var input = new JobCreationAndStatusUpdateActivity.JobCancelledInputWithAttemptNumber(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
          failureSummary);

      Mockito.doThrow(new IOException(EXCEPTION_MESSAGE)).when(jobsApi).persistJobCancellation(any());

      assertThrows(
          RetryableException.class,
          () -> jobCreationAndStatusUpdateActivity.jobCancelledWithAttemptNumber(input));
    }

    @Test
    void ensureCleanJobStateHappyPath() {
      when(airbyteApiClient.getJobsApi()).thenReturn(jobsApi);
      assertDoesNotThrow(
          () -> jobCreationAndStatusUpdateActivity.ensureCleanJobState(new EnsureCleanJobStateInput(CONNECTION_ID)));
    }

    @Test
    void ensureCleanJobStateThrowsRetryableOnApiFailure() throws IOException {
      when(airbyteApiClient.getJobsApi()).thenReturn(jobsApi);
      Mockito.doThrow(new IOException(EXCEPTION_MESSAGE)).when(jobsApi).failNonTerminalJobs(any());

      assertThrows(
          RetryableException.class,
          () -> jobCreationAndStatusUpdateActivity.ensureCleanJobState(new EnsureCleanJobStateInput(CONNECTION_ID)));
    }

  }

}
