/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.AttemptApi
import io.airbyte.api.client.generated.JobsApi
import io.airbyte.api.client.model.generated.AttemptInfoRead
import io.airbyte.api.client.model.generated.BooleanRead
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.ConnectionJobRequestBody
import io.airbyte.api.client.model.generated.CreateNewAttemptNumberRequest
import io.airbyte.api.client.model.generated.CreateNewAttemptNumberResponse
import io.airbyte.api.client.model.generated.FailAttemptRequest
import io.airbyte.api.client.model.generated.JobConfigType
import io.airbyte.api.client.model.generated.JobCreate
import io.airbyte.api.client.model.generated.JobFailureRequest
import io.airbyte.api.client.model.generated.JobInfoRead
import io.airbyte.api.client.model.generated.JobRead
import io.airbyte.api.client.model.generated.JobStatus
import io.airbyte.api.client.model.generated.JobSuccessWithAttemptNumberRequest
import io.airbyte.api.client.model.generated.PersistCancelJobRequestBody
import io.airbyte.commons.temporal.exception.RetryableException
import io.airbyte.config.AttemptFailureSummary
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.State
import io.airbyte.featureflag.TestClient
import io.airbyte.workers.storage.activities.OutputStorageClient
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.AttemptCreationInput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.AttemptNumberFailureInput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.EnsureCleanJobStateInput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobCancelledInputWithAttemptNumber
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobCheckFailureInput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobCreationInput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobFailureInput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobSuccessInputWithAttemptNumber
import org.assertj.core.api.ThrowableAssert
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.io.IOException
import java.util.UUID

@ExtendWith(MockitoExtension::class)
internal class JobCreationAndStatusUpdateActivityTest {
  @Mock
  private lateinit var airbyteApiClient: AirbyteApiClient

  @Mock
  private lateinit var jobsApi: JobsApi

  @Mock
  private lateinit var attemptApi: AttemptApi

  @Mock
  private lateinit var featureFlagClient: TestClient

  @Mock
  private lateinit var outputStateStorageClient: OutputStorageClient<State>

  @Mock
  private lateinit var outputCatalogStorageClient: OutputStorageClient<ConfiguredAirbyteCatalog>

  private lateinit var jobCreationAndStatusUpdateActivity: JobCreationAndStatusUpdateActivityImpl

  @BeforeEach
  fun beforeEach() {
    jobCreationAndStatusUpdateActivity =
      JobCreationAndStatusUpdateActivityImpl(
        airbyteApiClient,
        featureFlagClient,
        outputStateStorageClient,
        outputCatalogStorageClient,
      )
  }

  @Nested
  internal inner class Creation {
    @Test
    @DisplayName("Test job creation")
    @Throws(IOException::class)
    fun createJob() {
      whenever(airbyteApiClient.jobsApi).thenReturn(jobsApi)
      whenever(jobsApi.createJob(JobCreate(CONNECTION_ID, true)))
        .thenReturn(
          JobInfoRead(
            JobRead(
              JOB_ID,
              JobConfigType.SYNC,
              CONNECTION_ID.toString(),
              System.currentTimeMillis(),
              System.currentTimeMillis(),
              JobStatus.SUCCEEDED,
              null,
              null,
              null,
              null,
              null,
              null,
            ),
            mutableListOf<AttemptInfoRead>(),
          ),
        )
      val newJob = jobCreationAndStatusUpdateActivity.createNewJob(JobCreationInput(CONNECTION_ID, true))

      Assertions.assertEquals(JOB_ID, newJob.jobId)
    }

    @Test
    @DisplayName("Test job creation throws retryable exception")
    @Throws(IOException::class)
    fun createJobThrows() {
      whenever(airbyteApiClient.jobsApi).thenReturn(jobsApi)
      whenever(jobsApi.createJob(any<JobCreate>())).thenThrow(IOException())
      Assertions.assertThrows<RetryableException?>(
        RetryableException::class.java,
        Executable {
          jobCreationAndStatusUpdateActivity.createNewJob(
            JobCreationInput(
              CONNECTION_ID,
              true,
            ),
          )
        },
      )
    }

    @ParameterizedTest
    @ValueSource(ints = [1, 2, 3, 5, 20, 30, 1439, 11])
    fun isLastJobOrAttemptFailureReturnsTrueIfNotFirstAttemptForJob(attemptNumber: Int) {
      val input =
        JobCheckFailureInput(
          JOB_ID,
          attemptNumber,
          CONNECTION_ID,
        )
      val result = jobCreationAndStatusUpdateActivity.isLastJobOrAttemptFailure(input)

      Assertions.assertTrue(result)
    }

    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    @Throws(IOException::class)
    fun isLastJobOrAttemptFailureReturnsChecksPreviousJobIfFirstAttempt(didSucceed: Boolean) {
      val input =
        JobCheckFailureInput(
          JOB_ID,
          0,
          CONNECTION_ID,
        )

      whenever(airbyteApiClient.jobsApi).thenReturn(jobsApi)
      whenever(jobsApi.didPreviousJobSucceed(any<ConnectionJobRequestBody>()))
        .thenReturn(BooleanRead(didSucceed))

      val result = jobCreationAndStatusUpdateActivity.isLastJobOrAttemptFailure(input)

      Assertions.assertEquals(!didSucceed, result)
    }

    @Test
    @Throws(IOException::class)
    fun isLastJobOrAttemptFailureThrowsRetryableErrorIfApiCallFails() {
      val input =
        JobCheckFailureInput(
          JOB_ID,
          0,
          CONNECTION_ID,
        )

      whenever(airbyteApiClient.jobsApi).thenReturn(jobsApi)
      whenever(jobsApi.didPreviousJobSucceed(any<ConnectionJobRequestBody>()))
        .thenThrow(IOException(EXCEPTION_MESSAGE))

      Assertions.assertThrows<RetryableException?>(
        RetryableException::class.java,
        Executable { jobCreationAndStatusUpdateActivity.isLastJobOrAttemptFailure(input) },
      )
    }

    @Test
    @DisplayName("Test attempt creation")
    @Throws(IOException::class)
    fun createAttemptNumber() {
      whenever(airbyteApiClient.attemptApi).thenReturn(attemptApi)
      whenever(attemptApi.createNewAttemptNumber(CreateNewAttemptNumberRequest(JOB_ID)))
        .thenReturn(CreateNewAttemptNumberResponse(ATTEMPT_NUMBER_1))

      val output = jobCreationAndStatusUpdateActivity.createNewAttemptNumber(AttemptCreationInput(JOB_ID))
      org.assertj.core.api.Assertions
        .assertThat(output.attemptNumber)
        .isEqualTo(ATTEMPT_NUMBER_1)
    }

    @Test
    @DisplayName("Test exception errors are properly wrapped")
    @Throws(IOException::class)
    fun createAttemptNumberThrowException() {
      whenever(airbyteApiClient.attemptApi).thenReturn(attemptApi)
      whenever(attemptApi.createNewAttemptNumber(CreateNewAttemptNumberRequest(JOB_ID)))
        .thenThrow(IOException())

      org.assertj.core.api.Assertions
        .assertThatThrownBy(
          ThrowableAssert.ThrowingCallable {
            jobCreationAndStatusUpdateActivity.createNewAttemptNumber(
              AttemptCreationInput(
                JOB_ID,
              ),
            )
          },
        ).isInstanceOf(RetryableException::class.java)
        .hasCauseInstanceOf(IOException::class.java)
    }
  }

  @Nested
  internal inner class Update {
    @Test
    @Throws(IOException::class)
    fun setJobSuccess() {
      whenever(airbyteApiClient.jobsApi).thenReturn(jobsApi)
      val request =
        JobSuccessInputWithAttemptNumber(JOB_ID, ATTEMPT_NUMBER, CONNECTION_ID, standardSyncOutput)
      jobCreationAndStatusUpdateActivity.jobSuccessWithAttemptNumber(request)
      verify(jobsApi).jobSuccessWithAttemptNumber(
        JobSuccessWithAttemptNumberRequest(
          request.jobId!!,
          request.attemptNumber!!,
          request.connectionId!!,
          request.standardSyncOutput!!,
        ),
      )
    }

    @Test
    fun setJobSuccessNullJobId() {
      val request =
        JobSuccessInputWithAttemptNumber(null, ATTEMPT_NUMBER, CONNECTION_ID, standardSyncOutput)
      Assertions.assertDoesNotThrow(Executable { jobCreationAndStatusUpdateActivity.jobSuccessWithAttemptNumber(request) })
    }

    @Test
    @Throws(IOException::class)
    fun setJobSuccessWrapException() {
      whenever(airbyteApiClient.jobsApi).thenReturn(jobsApi)
      val exception: IOException = IOException(TEST_EXCEPTION_MESSAGE)
      doThrow(exception)
        .`when`(jobsApi)
        .jobSuccessWithAttemptNumber(any<JobSuccessWithAttemptNumberRequest>())

      org.assertj.core.api.Assertions
        .assertThatThrownBy(
          ThrowableAssert.ThrowingCallable {
            jobCreationAndStatusUpdateActivity.jobSuccessWithAttemptNumber(
              JobSuccessInputWithAttemptNumber(JOB_ID, ATTEMPT_NUMBER, CONNECTION_ID, standardSyncOutput),
            )
          },
        ).isInstanceOf(RetryableException::class.java)
        .hasCauseInstanceOf(IOException::class.java)
    }

    @Test
    @Throws(IOException::class)
    fun setJobFailure() {
      whenever(airbyteApiClient.jobsApi).thenReturn(jobsApi)
      jobCreationAndStatusUpdateActivity.jobFailure(JobFailureInput(JOB_ID, 1, CONNECTION_ID, REASON))
      verify(jobsApi).jobFailure(JobFailureRequest(JOB_ID, 1, CONNECTION_ID, REASON))
    }

    @Test
    fun setJobFailureNullJobId() {
      Assertions.assertDoesNotThrow(
        Executable {
          jobCreationAndStatusUpdateActivity.jobFailure(JobFailureInput(null, 1, CONNECTION_ID, REASON))
        },
      )
    }

    @Test
    @Throws(IOException::class)
    fun setJobFailureWithNullJobSyncConfig() {
      whenever(airbyteApiClient.jobsApi).thenReturn(jobsApi)
      whenever(jobsApi.jobFailure(any<JobFailureRequest>())).thenThrow(IOException())

      org.assertj.core.api.Assertions
        .assertThatThrownBy(
          ThrowableAssert.ThrowingCallable {
            jobCreationAndStatusUpdateActivity.jobFailure(
              JobFailureInput(
                JOB_ID,
                1,
                CONNECTION_ID,
                REASON,
              ),
            )
          },
        ).isInstanceOf(RetryableException::class.java)
        .hasCauseInstanceOf(IOException::class.java)
      verify(jobsApi).jobFailure(JobFailureRequest(JOB_ID, 1, CONNECTION_ID, REASON))
    }

    @Test
    fun attemptFailureWithAttemptNumberHappyPath() {
      whenever(airbyteApiClient.attemptApi).thenReturn(attemptApi)
      val input =
        AttemptNumberFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
          standardSyncOutput,
          failureSummary,
        )

      Assertions.assertDoesNotThrow(
        Executable { jobCreationAndStatusUpdateActivity.attemptFailureWithAttemptNumber(input) },
      )
    }

    @Test
    fun attemptFailureWithAttemptNumberNullJobId() {
      val input =
        AttemptNumberFailureInput(
          null,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
          standardSyncOutput,
          failureSummary,
        )

      Assertions.assertDoesNotThrow(
        Executable { jobCreationAndStatusUpdateActivity.attemptFailureWithAttemptNumber(input) },
      )
    }

    @Test
    @Throws(IOException::class)
    fun attemptFailureWithAttemptNumberThrowsRetryableOnApiFailure() {
      whenever(airbyteApiClient.attemptApi).thenReturn(attemptApi)
      val input =
        AttemptNumberFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
          standardSyncOutput,
          failureSummary,
        )

      doThrow(IOException(EXCEPTION_MESSAGE)).`when`(attemptApi).failAttempt(
        any<FailAttemptRequest>(),
      )

      Assertions.assertThrows<RetryableException>(
        RetryableException::class.java,
        Executable { jobCreationAndStatusUpdateActivity.attemptFailureWithAttemptNumber(input) },
      )
    }

    @Test
    @Throws(IOException::class)
    fun cancelJobHappyPath() {
      whenever(airbyteApiClient.jobsApi).thenReturn(jobsApi)
      val input =
        JobCancelledInputWithAttemptNumber(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
          failureSummary,
        )

      val jobReq = argumentCaptor<PersistCancelJobRequestBody>()

      jobCreationAndStatusUpdateActivity.jobCancelledWithAttemptNumber(input)

      verify(jobsApi).persistJobCancellation(jobReq.capture())
      Assertions.assertEquals(JOB_ID, jobReq.firstValue.jobId)
      Assertions.assertEquals(ATTEMPT_NUMBER, jobReq.firstValue.attemptNumber)
      Assertions.assertEquals(CONNECTION_ID, jobReq.firstValue.connectionId)
      Assertions.assertEquals(failureSummary, jobReq.firstValue.attemptFailureSummary)
    }

    @Test
    @Throws(IOException::class)
    fun cancelJobThrowsRetryableOnJobsApiFailure() {
      whenever(airbyteApiClient.jobsApi).thenReturn(jobsApi)
      val input =
        JobCancelledInputWithAttemptNumber(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
          failureSummary,
        )

      doThrow(IOException(EXCEPTION_MESSAGE))
        .`when`(jobsApi)
        .persistJobCancellation(any<PersistCancelJobRequestBody>())

      Assertions.assertThrows<RetryableException?>(
        RetryableException::class.java,
        Executable { jobCreationAndStatusUpdateActivity.jobCancelledWithAttemptNumber(input) },
      )
    }

    @Test
    fun ensureCleanJobStateHappyPath() {
      whenever(airbyteApiClient.jobsApi).thenReturn(jobsApi)
      Assertions.assertDoesNotThrow(
        Executable { jobCreationAndStatusUpdateActivity.ensureCleanJobState(EnsureCleanJobStateInput(CONNECTION_ID)) },
      )
    }

    @Test
    @Throws(IOException::class)
    fun ensureCleanJobStateThrowsRetryableOnApiFailure() {
      whenever(airbyteApiClient.jobsApi).thenReturn(jobsApi)
      doThrow(IOException(EXCEPTION_MESSAGE)).`when`(jobsApi).failNonTerminalJobs(
        any<ConnectionIdRequestBody>(),
      )

      Assertions.assertThrows<RetryableException>(
        RetryableException::class.java,
        Executable { jobCreationAndStatusUpdateActivity.ensureCleanJobState(EnsureCleanJobStateInput(CONNECTION_ID)) },
      )
    }
  }

  companion object {
    const val REASON: String = "reason"
    private const val EXCEPTION_MESSAGE = "bang"

    private val CONNECTION_ID: UUID = UUID.randomUUID()
    private const val JOB_ID = 123L
    private const val ATTEMPT_NUMBER = 0
    private const val ATTEMPT_NUMBER_1 = 1

    private const val TEST_EXCEPTION_MESSAGE = "test"

    private val standardSyncOutput: StandardSyncOutput? =
      StandardSyncOutput()
        .withStandardSyncSummary(
          StandardSyncSummary()
            .withStatus(StandardSyncSummary.ReplicationStatus.COMPLETED),
        )

    private val failureSummary: AttemptFailureSummary? =
      AttemptFailureSummary()
        .withFailures(
          mutableListOf<FailureReason?>(
            FailureReason()
              .withFailureOrigin(FailureReason.FailureOrigin.SOURCE),
          ),
        )
  }
}
