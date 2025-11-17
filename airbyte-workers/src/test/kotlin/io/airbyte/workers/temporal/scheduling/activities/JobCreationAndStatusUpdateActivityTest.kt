/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.AttemptApi
import io.airbyte.api.client.generated.ConnectionApi
import io.airbyte.api.client.generated.JobsApi
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
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.io.IOException
import java.util.UUID

internal class JobCreationAndStatusUpdateActivityTest {
  private lateinit var airbyteApiClient: AirbyteApiClient
  private lateinit var jobsApi: JobsApi
  private lateinit var attemptApi: AttemptApi
  private lateinit var connectionApi: ConnectionApi
  private lateinit var featureFlagClient: TestClient
  private lateinit var outputStateStorageClient: OutputStorageClient<State>
  private lateinit var jobCreationAndStatusUpdateActivity: JobCreationAndStatusUpdateActivityImpl

  @BeforeEach
  fun beforeEach() {
    airbyteApiClient = mockk()
    jobsApi = mockk(relaxed = true)
    attemptApi = mockk(relaxed = true)
    connectionApi = mockk()
    featureFlagClient = mockk(relaxed = true)
    outputStateStorageClient = mockk()
    jobCreationAndStatusUpdateActivity =
      JobCreationAndStatusUpdateActivityImpl(
        airbyteApiClient,
        featureFlagClient,
        outputStateStorageClient,
      )
  }

  @Nested
  internal inner class Creation {
    @Test
    @DisplayName("Test job creation")
    fun createJob() {
      every { airbyteApiClient.jobsApi } returns jobsApi
      every { jobsApi.createJob(JobCreate(CONNECTION_ID, true)) } returns
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
          mutableListOf(),
        )
      val newJob = jobCreationAndStatusUpdateActivity.createNewJob(JobCreationInput(CONNECTION_ID, true))

      Assertions.assertEquals(JOB_ID, newJob.jobId)
    }

    @Test
    @DisplayName("Test job creation throws retryable exception")
    fun createJobThrows() {
      every { airbyteApiClient.jobsApi } returns jobsApi
      every { jobsApi.createJob(any<JobCreate>()) } answers { throw IOException() }
      Assertions.assertThrows(
        RetryableException::class.java,
      ) {
        jobCreationAndStatusUpdateActivity.createNewJob(
          JobCreationInput(
            CONNECTION_ID,
            true,
          ),
        )
      }
    }

    // shouldRunSourceCheck tests
    @Test
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

      every { airbyteApiClient.jobsApi } returns jobsApi
      every { jobsApi.getJobInfoLight(JobIdRequestBody(JOB_ID)) } returns
        jobInfoLight

      val result = jobCreationAndStatusUpdateActivity.shouldRunSourceCheck(input)

      Assertions.assertFalse(result)
    }

    @Test
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

      every { airbyteApiClient.jobsApi } returns jobsApi
      every { jobsApi.getJobInfoLight(JobIdRequestBody(JOB_ID)) } returns
        jobInfoLight
      every { jobsApi.didPreviousJobSucceed(any<ConnectionJobRequestBody>()) } returns
        BooleanRead(false)

      val result = jobCreationAndStatusUpdateActivity.shouldRunSourceCheck(input)

      Assertions.assertTrue(result)
    }

    @Test
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

      every { airbyteApiClient.jobsApi } returns jobsApi
      every { jobsApi.getJobInfoLight(JobIdRequestBody(JOB_ID)) } returns
        jobInfoLight
      every { jobsApi.didPreviousJobSucceed(any<ConnectionJobRequestBody>()) } returns
        BooleanRead(true)

      val result = jobCreationAndStatusUpdateActivity.shouldRunSourceCheck(input)

      Assertions.assertFalse(result)
    }

    @Test
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

      every { airbyteApiClient.jobsApi } returns jobsApi
      every { jobsApi.getJobInfoLight(JobIdRequestBody(JOB_ID)) } returns
        jobInfoLight

      val result = jobCreationAndStatusUpdateActivity.shouldRunSourceCheck(input)

      Assertions.assertTrue(result)
    }

    @Test
    fun shouldRunSourceCheckHandlesApiFailureGracefully() {
      val input =
        JobCheckFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
        )

      every { airbyteApiClient.jobsApi } returns jobsApi
      every { jobsApi.getJobInfoLight(any<JobIdRequestBody>()) } answers { throw IOException(TEST_EXCEPTION_MESSAGE) }

      // When the API fails to get job info, it should fall back to checking previous job status
      // For the first attempt, it should check previous job status
      every { jobsApi.didPreviousJobSucceed(any<ConnectionJobRequestBody>()) } returns
        BooleanRead(false)

      val result = jobCreationAndStatusUpdateActivity.shouldRunSourceCheck(input)

      Assertions.assertTrue(result)
    }

    @Test
    fun shouldRunSourceCheckReturnsFalseWhenFeatureFlagEnabled() {
      val input =
        JobCheckFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
        )
      val workspaceId = UUID.randomUUID()

      // Mock the connection API to return workspace ID
      every { airbyteApiClient.connectionApi } returns connectionApi
      every { connectionApi.getConnectionContext(ConnectionIdRequestBody(CONNECTION_ID)) } returns
        ConnectionContextRead(CONNECTION_ID, null, null, null, null, workspaceId, null)

      // Enable the feature flag
      every {
        featureFlagClient.boolVariation(
          SkipCheckBeforeSync,
          Multi(listOf(Connection(CONNECTION_ID), Workspace(workspaceId))),
        )
      } returns true

      val result = jobCreationAndStatusUpdateActivity.shouldRunSourceCheck(input)

      Assertions.assertFalse(result)
    }

    @Test
    fun shouldRunSourceCheckReturnsIsLastJobOrAttemptFailureResultWhenFeatureFlagDisabled() {
      val input =
        JobCheckFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
        )
      val workspaceId = UUID.randomUUID()

      // Mock the connection API to return workspace ID
      every { airbyteApiClient.connectionApi } returns connectionApi
      every { connectionApi.getConnectionContext(ConnectionIdRequestBody(CONNECTION_ID)) } returns
        ConnectionContextRead(CONNECTION_ID, null, null, null, null, workspaceId, null)

      // Disable the feature flag
      every {
        featureFlagClient.boolVariation(
          SkipCheckBeforeSync,
          Multi(listOf(Connection(CONNECTION_ID), Workspace(workspaceId))),
        )
      } returns false

      // Mock previous job failed (so normal logic would return true)
      every { airbyteApiClient.jobsApi } returns jobsApi
      every { jobsApi.didPreviousJobSucceed(any<ConnectionJobRequestBody>()) } returns
        BooleanRead(false)

      val result = jobCreationAndStatusUpdateActivity.shouldRunSourceCheck(input)

      Assertions.assertTrue(result)
    }

    @Test
    fun shouldRunSourceCheckHandlesWorkspaceIdFetchFailureGracefully() {
      val input =
        JobCheckFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
        )

      // Mock the connection API to fail
      every { airbyteApiClient.connectionApi } returns connectionApi
      every { connectionApi.getConnectionContext(any<ConnectionIdRequestBody>()) } answers { throw IOException("Failed to fetch workspace") }

      // Feature flag check should still work with just connection context
      every {
        featureFlagClient.boolVariation(
          SkipCheckBeforeSync,
          Multi(listOf(Connection(CONNECTION_ID))),
        )
      } returns true

      val result = jobCreationAndStatusUpdateActivity.shouldRunSourceCheck(input)

      Assertions.assertFalse(result)
    }

    // shouldRunDestinationCheck tests
    @Test
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
    fun shouldRunDestinationCheckReturnsTrueOnFirstAttemptWithPreviousFailure() {
      val input =
        JobCheckFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
        )

      every { airbyteApiClient.jobsApi } returns jobsApi
      every { jobsApi.didPreviousJobSucceed(any<ConnectionJobRequestBody>()) } returns
        BooleanRead(false)

      val result = jobCreationAndStatusUpdateActivity.shouldRunDestinationCheck(input)

      Assertions.assertTrue(result)
    }

    @Test
    fun shouldRunDestinationCheckReturnsFalseOnFirstAttemptWithPreviousSuccess() {
      val input =
        JobCheckFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
        )

      every { airbyteApiClient.jobsApi } returns jobsApi
      every { jobsApi.didPreviousJobSucceed(any<ConnectionJobRequestBody>()) } returns
        BooleanRead(true)

      val result = jobCreationAndStatusUpdateActivity.shouldRunDestinationCheck(input)

      Assertions.assertFalse(result)
    }

    @Test
    fun shouldRunDestinationCheckReturnsFalseWhenFeatureFlagEnabled() {
      val input =
        JobCheckFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
        )
      val workspaceId = UUID.randomUUID()

      // Mock the connection API to return workspace ID
      every { airbyteApiClient.connectionApi } returns connectionApi
      every { connectionApi.getConnectionContext(ConnectionIdRequestBody(CONNECTION_ID)) } returns
        ConnectionContextRead(CONNECTION_ID, null, null, null, null, workspaceId, null)

      // Enable the feature flag
      every {
        featureFlagClient.boolVariation(
          SkipCheckBeforeSync,
          Multi(listOf(Connection(CONNECTION_ID), Workspace(workspaceId))),
        )
      } returns true

      val result = jobCreationAndStatusUpdateActivity.shouldRunDestinationCheck(input)

      Assertions.assertFalse(result)
    }

    @Test
    fun shouldRunDestinationCheckReturnsIsLastJobOrAttemptFailureResultWhenFeatureFlagDisabled() {
      val input =
        JobCheckFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
        )
      val workspaceId = UUID.randomUUID()

      // Mock the connection API to return workspace ID
      every { airbyteApiClient.connectionApi } returns connectionApi
      every { connectionApi.getConnectionContext(ConnectionIdRequestBody(CONNECTION_ID)) } returns
        ConnectionContextRead(CONNECTION_ID, null, null, null, null, workspaceId, null)

      // Disable the feature flag
      every {
        featureFlagClient.boolVariation(
          SkipCheckBeforeSync,
          Multi(listOf(Connection(CONNECTION_ID), Workspace(workspaceId))),
        )
      } returns false

      // Mock previous job failed (so normal logic would return true)
      every { airbyteApiClient.jobsApi } returns jobsApi
      every { jobsApi.didPreviousJobSucceed(any<ConnectionJobRequestBody>()) } returns
        BooleanRead(false)

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
    fun isLastJobOrAttemptFailureReturnsChecksPreviousJobIfFirstAttempt(didSucceed: Boolean) {
      // Test that first attempts to check previous job status (testing isLastJobOrAttemptFailure logic)
      val input =
        JobCheckFailureInput(
          JOB_ID,
          0,
          CONNECTION_ID,
        )

      every { airbyteApiClient.jobsApi } returns jobsApi
      every { jobsApi.didPreviousJobSucceed(any<ConnectionJobRequestBody>()) } returns
        BooleanRead(didSucceed)

      // Both checks should return the opposite of didSucceed
      val sourceResult = jobCreationAndStatusUpdateActivity.shouldRunSourceCheck(input)
      val destResult = jobCreationAndStatusUpdateActivity.shouldRunDestinationCheck(input)

      Assertions.assertEquals(!didSucceed, sourceResult)
      Assertions.assertEquals(!didSucceed, destResult)
    }

    @Test
    fun isLastJobOrAttemptFailureThrowsRetryableErrorIfApiCallFails() {
      // Test that API failures result in RetryableException (testing isLastJobOrAttemptFailure error handling)
      val input =
        JobCheckFailureInput(
          JOB_ID,
          0,
          CONNECTION_ID,
        )

      every { airbyteApiClient.jobsApi } returns jobsApi
      every { jobsApi.didPreviousJobSucceed(any<ConnectionJobRequestBody>()) } answers { throw IOException(EXCEPTION_MESSAGE) }

      // Both methods should throw RetryableException when the underlying API call fails
      Assertions.assertThrows(
        RetryableException::class.java,
      ) { jobCreationAndStatusUpdateActivity.shouldRunSourceCheck(input) }

      // Reset mock for second call
      every { jobsApi.didPreviousJobSucceed(any<ConnectionJobRequestBody>()) } answers { throw IOException(EXCEPTION_MESSAGE) }

      Assertions.assertThrows(
        RetryableException::class.java,
      ) { jobCreationAndStatusUpdateActivity.shouldRunDestinationCheck(input) }
    }

    @Test
    @DisplayName("Test attempt creation")
    fun createAttemptNumber() {
      every { airbyteApiClient.attemptApi } returns attemptApi
      every { attemptApi.createNewAttemptNumber(CreateNewAttemptNumberRequest(JOB_ID)) } returns
        CreateNewAttemptNumberResponse(ATTEMPT_NUMBER_1)

      val output = jobCreationAndStatusUpdateActivity.createNewAttemptNumber(AttemptCreationInput(JOB_ID))
      org.assertj.core.api.Assertions
        .assertThat(output.attemptNumber)
        .isEqualTo(ATTEMPT_NUMBER_1)
    }

    @Test
    @DisplayName("Test exception errors are properly wrapped")
    fun createAttemptNumberThrowException() {
      every { airbyteApiClient.attemptApi } returns attemptApi
      every { attemptApi.createNewAttemptNumber(CreateNewAttemptNumberRequest(JOB_ID)) } answers { throw IOException() }

      org.assertj.core.api.Assertions
        .assertThatThrownBy {
          jobCreationAndStatusUpdateActivity.createNewAttemptNumber(
            AttemptCreationInput(
              JOB_ID,
            ),
          )
        }.isInstanceOf(RetryableException::class.java)
        .hasCauseInstanceOf(IOException::class.java)
    }
  }

  @Nested
  internal inner class Update {
    @Test
    fun setJobSuccess() {
      every { airbyteApiClient.jobsApi } returns jobsApi
      every { jobsApi.jobSuccessWithAttemptNumber(any<JobSuccessWithAttemptNumberRequest>()) } returns mockk(relaxed = true)
      val request =
        JobSuccessInputWithAttemptNumber(JOB_ID, ATTEMPT_NUMBER, CONNECTION_ID, standardSyncOutput)
      jobCreationAndStatusUpdateActivity.jobSuccessWithAttemptNumber(request)
      verify {
        jobsApi.jobSuccessWithAttemptNumber(
          JobSuccessWithAttemptNumberRequest(
            request.jobId!!,
            request.attemptNumber!!,
            request.connectionId!!,
            request.standardSyncOutput!!,
          ),
        )
      }
    }

    @Test
    fun setJobSuccessNullJobId() {
      val request =
        JobSuccessInputWithAttemptNumber(null, ATTEMPT_NUMBER, CONNECTION_ID, standardSyncOutput)
      Assertions.assertDoesNotThrow { jobCreationAndStatusUpdateActivity.jobSuccessWithAttemptNumber(request) }
    }

    @Test
    fun setJobSuccessWrapException() {
      every { airbyteApiClient.jobsApi } returns jobsApi
      val exception = IOException(TEST_EXCEPTION_MESSAGE)
      every { jobsApi.jobSuccessWithAttemptNumber(any<JobSuccessWithAttemptNumberRequest>()) } throws exception

      org.assertj.core.api.Assertions
        .assertThatThrownBy {
          jobCreationAndStatusUpdateActivity.jobSuccessWithAttemptNumber(
            JobSuccessInputWithAttemptNumber(JOB_ID, ATTEMPT_NUMBER, CONNECTION_ID, standardSyncOutput),
          )
        }.isInstanceOf(RetryableException::class.java)
        .hasCauseInstanceOf(IOException::class.java)
    }

    @Test
    fun setJobFailure() {
      every { airbyteApiClient.jobsApi } returns jobsApi
      every { jobsApi.jobFailure(any<JobFailureRequest>()) } returns mockk(relaxed = true)
      jobCreationAndStatusUpdateActivity.jobFailure(JobFailureInput(JOB_ID, 1, CONNECTION_ID, REASON))
      verify { jobsApi.jobFailure(JobFailureRequest(JOB_ID, 1, CONNECTION_ID, REASON)) }
    }

    @Test
    fun setJobFailureNullJobId() {
      Assertions.assertDoesNotThrow {
        jobCreationAndStatusUpdateActivity.jobFailure(JobFailureInput(null, 1, CONNECTION_ID, REASON))
      }
    }

    @Test
    fun setJobFailureWithNullJobSyncConfig() {
      every { airbyteApiClient.jobsApi } returns jobsApi
      every { jobsApi.jobFailure(any<JobFailureRequest>()) } answers { throw IOException() }

      org.assertj.core.api.Assertions
        .assertThatThrownBy {
          jobCreationAndStatusUpdateActivity.jobFailure(
            JobFailureInput(
              JOB_ID,
              1,
              CONNECTION_ID,
              REASON,
            ),
          )
        }.isInstanceOf(RetryableException::class.java)
        .hasCauseInstanceOf(IOException::class.java)
      verify { jobsApi.jobFailure(JobFailureRequest(JOB_ID, 1, CONNECTION_ID, REASON)) }
    }

    @Test
    fun attemptFailureWithAttemptNumberHappyPath() {
      every { airbyteApiClient.attemptApi } returns attemptApi
      val input =
        AttemptNumberFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
          standardSyncOutput,
          failureSummary,
        )

      Assertions.assertDoesNotThrow { jobCreationAndStatusUpdateActivity.attemptFailureWithAttemptNumber(input) }
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

      Assertions.assertDoesNotThrow { jobCreationAndStatusUpdateActivity.attemptFailureWithAttemptNumber(input) }
    }

    @Test
    fun attemptFailureWithAttemptNumberThrowsRetryableOnApiFailure() {
      every { airbyteApiClient.attemptApi } returns attemptApi
      val input =
        AttemptNumberFailureInput(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
          standardSyncOutput,
          failureSummary,
        )

      every { attemptApi.failAttempt(any<FailAttemptRequest>()) } throws IOException(EXCEPTION_MESSAGE)

      Assertions.assertThrows(
        RetryableException::class.java,
      ) { jobCreationAndStatusUpdateActivity.attemptFailureWithAttemptNumber(input) }
    }

    @Test
    fun cancelJobHappyPath() {
      every { airbyteApiClient.jobsApi } returns jobsApi
      val input =
        JobCancelledInputWithAttemptNumber(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
          failureSummary,
        )

      val jobReq = slot<PersistCancelJobRequestBody>()

      jobCreationAndStatusUpdateActivity.jobCancelledWithAttemptNumber(input)

      verify { jobsApi.persistJobCancellation(capture(jobReq)) }
      Assertions.assertEquals(JOB_ID, jobReq.captured.jobId)
      Assertions.assertEquals(ATTEMPT_NUMBER, jobReq.captured.attemptNumber)
      Assertions.assertEquals(CONNECTION_ID, jobReq.captured.connectionId)
      Assertions.assertEquals(failureSummary, jobReq.captured.attemptFailureSummary)
    }

    @Test
    fun cancelJobThrowsRetryableOnJobsApiFailure() {
      every { airbyteApiClient.jobsApi } returns jobsApi
      val input =
        JobCancelledInputWithAttemptNumber(
          JOB_ID,
          ATTEMPT_NUMBER,
          CONNECTION_ID,
          failureSummary,
        )

      every { jobsApi.persistJobCancellation(any<PersistCancelJobRequestBody>()) } throws IOException(EXCEPTION_MESSAGE)

      Assertions.assertThrows(
        RetryableException::class.java,
      ) { jobCreationAndStatusUpdateActivity.jobCancelledWithAttemptNumber(input) }
    }

    @Test
    fun ensureCleanJobStateHappyPath() {
      every { airbyteApiClient.jobsApi } returns jobsApi
      Assertions.assertDoesNotThrow { jobCreationAndStatusUpdateActivity.ensureCleanJobState(EnsureCleanJobStateInput(CONNECTION_ID)) }
    }

    @Test
    fun ensureCleanJobStateThrowsRetryableOnApiFailure() {
      every { airbyteApiClient.jobsApi } returns jobsApi
      every { jobsApi.failNonTerminalJobs(any<ConnectionIdRequestBody>()) } throws IOException(EXCEPTION_MESSAGE)

      Assertions.assertThrows(
        RetryableException::class.java,
      ) { jobCreationAndStatusUpdateActivity.ensureCleanJobState(EnsureCleanJobStateInput(CONNECTION_ID)) }
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
