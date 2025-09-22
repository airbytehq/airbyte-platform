/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.AttemptApi
import io.airbyte.api.client.generated.ConnectionApi
import io.airbyte.api.client.generated.JobsApi
import io.airbyte.api.client.model.generated.AttemptInfoRead
import io.airbyte.api.client.model.generated.BooleanRead
import io.airbyte.api.client.model.generated.ConnectionContextRead
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.ConnectionJobRequestBody
import io.airbyte.api.client.model.generated.CreateNewAttemptNumberRequest
import io.airbyte.api.client.model.generated.CreateNewAttemptNumberResponse
import io.airbyte.api.client.model.generated.FailAttemptRequest
import io.airbyte.api.client.model.generated.JobConfigType
import io.airbyte.api.client.model.generated.JobCreate
import io.airbyte.api.client.model.generated.JobFailureRequest
import io.airbyte.api.client.model.generated.JobIdRequestBody
import io.airbyte.api.client.model.generated.JobInfoLightRead
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
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.SkipCheckBeforeSync
import io.airbyte.featureflag.TestClient
import io.airbyte.featureflag.Workspace
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
  private lateinit var connectionApi: ConnectionApi

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

    // shouldRunSourceCheck tests
    @Test
    @Throws(IOException::class)
    fun shouldRunSourceCheckReturnsFalseForResetJob() {
      val input =
        JobCheckFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
        )

      val jobRead =
        JobRead(
          id = JOB_ID,
          configType = JobConfigType.RESET_CONNECTION,
          configId = CONNECTION_ID.toString(),
          createdAt = 0L,
          updatedAt = 0L,
          status = JobStatus.RUNNING,
        )
      val jobInfoLight = JobInfoLightRead(jobRead)

      whenever(airbyteApiClient.jobsApi).thenReturn(jobsApi)
      whenever(jobsApi.getJobInfoLight(JobIdRequestBody(JOB_ID)))
        .thenReturn(jobInfoLight)

      val result = jobCreationAndStatusUpdateActivity.shouldRunSourceCheck(input)

      Assertions.assertFalse(result)
    }

    @Test
    @Throws(IOException::class)
    fun shouldRunSourceCheckReturnsTrueForSyncJobOnFirstAttemptWithPreviousFailure() {
      val input =
        JobCheckFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
        )

      val jobRead =
        JobRead(
          id = JOB_ID,
          configType = JobConfigType.SYNC,
          configId = CONNECTION_ID.toString(),
          createdAt = 0L,
          updatedAt = 0L,
          status = JobStatus.RUNNING,
        )
      val jobInfoLight = JobInfoLightRead(jobRead)

      whenever(airbyteApiClient.jobsApi).thenReturn(jobsApi)
      whenever(jobsApi.getJobInfoLight(JobIdRequestBody(JOB_ID)))
        .thenReturn(jobInfoLight)
      whenever(jobsApi.didPreviousJobSucceed(any<ConnectionJobRequestBody>()))
        .thenReturn(BooleanRead(false))

      val result = jobCreationAndStatusUpdateActivity.shouldRunSourceCheck(input)

      Assertions.assertTrue(result)
    }

    @Test
    @Throws(IOException::class)
    fun shouldRunSourceCheckReturnsFalseForSyncJobOnFirstAttemptWithPreviousSuccess() {
      val input =
        JobCheckFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
        )

      val jobRead =
        JobRead(
          id = JOB_ID,
          configType = JobConfigType.SYNC,
          configId = CONNECTION_ID.toString(),
          createdAt = 0L,
          updatedAt = 0L,
          status = JobStatus.RUNNING,
        )
      val jobInfoLight = JobInfoLightRead(jobRead)

      whenever(airbyteApiClient.jobsApi).thenReturn(jobsApi)
      whenever(jobsApi.getJobInfoLight(JobIdRequestBody(JOB_ID)))
        .thenReturn(jobInfoLight)
      whenever(jobsApi.didPreviousJobSucceed(any<ConnectionJobRequestBody>()))
        .thenReturn(BooleanRead(true))

      val result = jobCreationAndStatusUpdateActivity.shouldRunSourceCheck(input)

      Assertions.assertFalse(result)
    }

    @Test
    @Throws(IOException::class)
    fun shouldRunSourceCheckReturnsTrueForSyncJobOnRetryAttempt() {
      val input =
        JobCheckFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER_1, // Second attempt
          CONNECTION_ID,
        )

      val jobRead =
        JobRead(
          id = JOB_ID,
          configType = JobConfigType.SYNC,
          configId = CONNECTION_ID.toString(),
          createdAt = 0L,
          updatedAt = 0L,
          status = JobStatus.RUNNING,
        )
      val jobInfoLight = JobInfoLightRead(jobRead)

      whenever(airbyteApiClient.jobsApi).thenReturn(jobsApi)
      whenever(jobsApi.getJobInfoLight(JobIdRequestBody(JOB_ID)))
        .thenReturn(jobInfoLight)

      val result = jobCreationAndStatusUpdateActivity.shouldRunSourceCheck(input)

      Assertions.assertTrue(result)
    }

    @Test
    @Throws(IOException::class)
    fun shouldRunSourceCheckHandlesApiFailureGracefully() {
      val input =
        JobCheckFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
        )

      whenever(airbyteApiClient.jobsApi).thenReturn(jobsApi)
      whenever(jobsApi.getJobInfoLight(any<JobIdRequestBody>()))
        .thenThrow(IOException(TEST_EXCEPTION_MESSAGE))

      // When API fails to get job info, it should fall back to checking previous job status
      // For first attempt, it should check previous job status
      whenever(jobsApi.didPreviousJobSucceed(any<ConnectionJobRequestBody>()))
        .thenReturn(BooleanRead(false))

      val result = jobCreationAndStatusUpdateActivity.shouldRunSourceCheck(input)

      Assertions.assertTrue(result)
    }

    @Test
    @Throws(IOException::class)
    fun shouldRunSourceCheckReturnsFalseWhenFeatureFlagEnabled() {
      val input =
        JobCheckFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
        )
      val workspaceId = UUID.randomUUID()

      // Mock the connection API to return workspace ID
      whenever(airbyteApiClient.connectionApi).thenReturn(connectionApi)
      whenever(connectionApi.getConnectionContext(ConnectionIdRequestBody(CONNECTION_ID)))
        .thenReturn(ConnectionContextRead(CONNECTION_ID, null, null, null, null, workspaceId, null))

      // Enable the feature flag
      whenever(
        featureFlagClient.boolVariation(
          SkipCheckBeforeSync,
          Multi(listOf(Connection(CONNECTION_ID), Workspace(workspaceId))),
        ),
      ).thenReturn(true)

      val result = jobCreationAndStatusUpdateActivity.shouldRunSourceCheck(input)

      Assertions.assertFalse(result)
    }

    @Test
    @Throws(IOException::class)
    fun shouldRunSourceCheckReturnsIsLastJobOrAttemptFailureResultWhenFeatureFlagDisabled() {
      val input =
        JobCheckFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
        )
      val workspaceId = UUID.randomUUID()

      // Mock the connection API to return workspace ID
      whenever(airbyteApiClient.connectionApi).thenReturn(connectionApi)
      whenever(connectionApi.getConnectionContext(ConnectionIdRequestBody(CONNECTION_ID)))
        .thenReturn(ConnectionContextRead(CONNECTION_ID, null, null, null, null, workspaceId, null))

      // Disable the feature flag
      whenever(
        featureFlagClient.boolVariation(
          SkipCheckBeforeSync,
          Multi(listOf(Connection(CONNECTION_ID), Workspace(workspaceId))),
        ),
      ).thenReturn(false)

      // Mock previous job failed (so normal logic would return true)
      whenever(airbyteApiClient.jobsApi).thenReturn(jobsApi)
      whenever(jobsApi.didPreviousJobSucceed(any<ConnectionJobRequestBody>()))
        .thenReturn(BooleanRead(false))

      val result = jobCreationAndStatusUpdateActivity.shouldRunSourceCheck(input)

      Assertions.assertTrue(result)
    }

    @Test
    @Throws(IOException::class)
    fun shouldRunSourceCheckHandlesWorkspaceIdFetchFailureGracefully() {
      val input =
        JobCheckFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
        )

      // Mock the connection API to fail
      whenever(airbyteApiClient.connectionApi).thenReturn(connectionApi)
      whenever(connectionApi.getConnectionContext(any<ConnectionIdRequestBody>()))
        .thenThrow(IOException("Failed to fetch workspace"))

      // Feature flag check should still work with just connection context
      whenever(
        featureFlagClient.boolVariation(
          SkipCheckBeforeSync,
          Multi(listOf(Connection(CONNECTION_ID))),
        ),
      ).thenReturn(true)

      val result = jobCreationAndStatusUpdateActivity.shouldRunSourceCheck(input)

      Assertions.assertFalse(result)
    }

    // shouldRunDestinationCheck tests
    @Test
    @Throws(IOException::class)
    fun shouldRunDestinationCheckReturnsTrueOnRetryAttempt() {
      val input =
        JobCheckFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER_1, // Second attempt
          CONNECTION_ID,
        )

      val result = jobCreationAndStatusUpdateActivity.shouldRunDestinationCheck(input)

      Assertions.assertTrue(result)
    }

    @Test
    @Throws(IOException::class)
    fun shouldRunDestinationCheckReturnsTrueOnFirstAttemptWithPreviousFailure() {
      val input =
        JobCheckFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
        )

      whenever(airbyteApiClient.jobsApi).thenReturn(jobsApi)
      whenever(jobsApi.didPreviousJobSucceed(any<ConnectionJobRequestBody>()))
        .thenReturn(BooleanRead(false))

      val result = jobCreationAndStatusUpdateActivity.shouldRunDestinationCheck(input)

      Assertions.assertTrue(result)
    }

    @Test
    @Throws(IOException::class)
    fun shouldRunDestinationCheckReturnsFalseOnFirstAttemptWithPreviousSuccess() {
      val input =
        JobCheckFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
        )

      whenever(airbyteApiClient.jobsApi).thenReturn(jobsApi)
      whenever(jobsApi.didPreviousJobSucceed(any<ConnectionJobRequestBody>()))
        .thenReturn(BooleanRead(true))

      val result = jobCreationAndStatusUpdateActivity.shouldRunDestinationCheck(input)

      Assertions.assertFalse(result)
    }

    @Test
    @Throws(IOException::class)
    fun shouldRunDestinationCheckReturnsFalseWhenFeatureFlagEnabled() {
      val input =
        JobCheckFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
        )
      val workspaceId = UUID.randomUUID()

      // Mock the connection API to return workspace ID
      whenever(airbyteApiClient.connectionApi).thenReturn(connectionApi)
      whenever(connectionApi.getConnectionContext(ConnectionIdRequestBody(CONNECTION_ID)))
        .thenReturn(ConnectionContextRead(CONNECTION_ID, null, null, null, null, workspaceId, null))

      // Enable the feature flag
      whenever(
        featureFlagClient.boolVariation(
          SkipCheckBeforeSync,
          Multi(listOf(Connection(CONNECTION_ID), Workspace(workspaceId))),
        ),
      ).thenReturn(true)

      val result = jobCreationAndStatusUpdateActivity.shouldRunDestinationCheck(input)

      Assertions.assertFalse(result)
    }

    @Test
    @Throws(IOException::class)
    fun shouldRunDestinationCheckReturnsIsLastJobOrAttemptFailureResultWhenFeatureFlagDisabled() {
      val input =
        JobCheckFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
        )
      val workspaceId = UUID.randomUUID()

      // Mock the connection API to return workspace ID
      whenever(airbyteApiClient.connectionApi).thenReturn(connectionApi)
      whenever(connectionApi.getConnectionContext(ConnectionIdRequestBody(CONNECTION_ID)))
        .thenReturn(ConnectionContextRead(CONNECTION_ID, null, null, null, null, workspaceId, null))

      // Disable the feature flag
      whenever(
        featureFlagClient.boolVariation(
          SkipCheckBeforeSync,
          Multi(listOf(Connection(CONNECTION_ID), Workspace(workspaceId))),
        ),
      ).thenReturn(false)

      // Mock previous job failed (so normal logic would return true)
      whenever(airbyteApiClient.jobsApi).thenReturn(jobsApi)
      whenever(jobsApi.didPreviousJobSucceed(any<ConnectionJobRequestBody>()))
        .thenReturn(BooleanRead(false))

      val result = jobCreationAndStatusUpdateActivity.shouldRunDestinationCheck(input)

      Assertions.assertTrue(result)
    }

    @ParameterizedTest
    @ValueSource(ints = [1, 2, 3, 5, 20, 30, 1439, 11])
    fun isLastJobOrAttemptFailureReturnsTrueIfNotFirstAttemptForJob(attemptNumber: Int) {
      // Test that non-first attempts always trigger checks (testing isLastJobOrAttemptFailure logic)
      val input =
        JobCheckFailureInput(
          JOB_ID,
          attemptNumber,
          CONNECTION_ID,
        )

      // Both shouldRunSourceCheck and shouldRunDestinationCheck should return true for non-first attempts
      val sourceResult = jobCreationAndStatusUpdateActivity.shouldRunSourceCheck(input)
      val destResult = jobCreationAndStatusUpdateActivity.shouldRunDestinationCheck(input)

      Assertions.assertTrue(sourceResult)
      Assertions.assertTrue(destResult)
    }

    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    @Throws(IOException::class)
    fun isLastJobOrAttemptFailureReturnsChecksPreviousJobIfFirstAttempt(didSucceed: Boolean) {
      // Test that first attempts check previous job status (testing isLastJobOrAttemptFailure logic)
      val input =
        JobCheckFailureInput(
          JOB_ID,
          0,
          CONNECTION_ID,
        )

      whenever(airbyteApiClient.jobsApi).thenReturn(jobsApi)
      whenever(jobsApi.didPreviousJobSucceed(any<ConnectionJobRequestBody>()))
        .thenReturn(BooleanRead(didSucceed))

      // Both checks should return the opposite of didSucceed
      val sourceResult = jobCreationAndStatusUpdateActivity.shouldRunSourceCheck(input)
      val destResult = jobCreationAndStatusUpdateActivity.shouldRunDestinationCheck(input)

      Assertions.assertEquals(!didSucceed, sourceResult)
      Assertions.assertEquals(!didSucceed, destResult)
    }

    @Test
    @Throws(IOException::class)
    fun isLastJobOrAttemptFailureThrowsRetryableErrorIfApiCallFails() {
      // Test that API failures result in RetryableException (testing isLastJobOrAttemptFailure error handling)
      val input =
        JobCheckFailureInput(
          JOB_ID,
          0,
          CONNECTION_ID,
        )

      whenever(airbyteApiClient.jobsApi).thenReturn(jobsApi)
      whenever(jobsApi.didPreviousJobSucceed(any<ConnectionJobRequestBody>()))
        .thenThrow(IOException(EXCEPTION_MESSAGE))

      // Both methods should throw RetryableException when the underlying API call fails
      Assertions.assertThrows<RetryableException?>(
        RetryableException::class.java,
        Executable { jobCreationAndStatusUpdateActivity.shouldRunSourceCheck(input) },
      )

      // Reset mock for second call
      whenever(jobsApi.didPreviousJobSucceed(any<ConnectionJobRequestBody>()))
        .thenThrow(IOException(EXCEPTION_MESSAGE))

      Assertions.assertThrows<RetryableException?>(
        RetryableException::class.java,
        Executable { jobCreationAndStatusUpdateActivity.shouldRunDestinationCheck(input) },
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
