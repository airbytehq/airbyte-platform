/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import datadog.trace.api.Trace
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.ConnectionJobRequestBody
import io.airbyte.api.client.model.generated.CreateNewAttemptNumberRequest
import io.airbyte.api.client.model.generated.FailAttemptRequest
import io.airbyte.api.client.model.generated.JobCreate
import io.airbyte.api.client.model.generated.JobFailureRequest
import io.airbyte.api.client.model.generated.JobSuccessWithAttemptNumberRequest
import io.airbyte.api.client.model.generated.PersistCancelJobRequestBody
import io.airbyte.api.client.model.generated.ReportJobStartRequest
import io.airbyte.commons.micronaut.EnvConstants
import io.airbyte.commons.temporal.exception.RetryableException
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.State
import io.airbyte.featureflag.AlwaysRunCheckBeforeSync
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME
import io.airbyte.metrics.lib.ApmTraceUtils.addExceptionToTrace
import io.airbyte.workers.context.AttemptContext
import io.airbyte.workers.storage.activities.OutputStorageClient
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.AttemptCreationInput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.AttemptNumberCreationOutput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.AttemptNumberFailureInput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.EnsureCleanJobStateInput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobCancelledInputWithAttemptNumber
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobCheckFailureInput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobCreationInput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobCreationOutput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobFailureInput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobSuccessInputWithAttemptNumber
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.ReportJobStartInput
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpStatus
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.lang.invoke.MethodHandles
import java.util.UUID

/**
 * JobCreationAndStatusUpdateActivityImpl.
 */
@Singleton
@Requires(env = [EnvConstants.CONTROL_PLANE])
class JobCreationAndStatusUpdateActivityImpl(
  private val airbyteApiClient: AirbyteApiClient,
  private val featureFlagClient: FeatureFlagClient,
  @param:Named("outputStateClient") private val stateClient: OutputStorageClient<State>?,
  @param:Named("outputCatalogClient") private val catalogClient: OutputStorageClient<ConfiguredAirbyteCatalog>?,
) : JobCreationAndStatusUpdateActivity {
  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  override fun createNewJob(input: JobCreationInput): JobCreationOutput {
    AttemptContext(input.connectionId, null, null).addTagsToTrace()
    try {
      val jobInfoRead = airbyteApiClient.jobsApi.createJob(JobCreate(input.connectionId!!, input.isScheduled))
      return JobCreationOutput(jobInfoRead.job.id)
    } catch (e: ClientException) {
      if (e.statusCode == HttpStatus.NOT_FOUND.getCode()) {
        throw e
      }
      log.error("Unable to create job for connection {}", input.connectionId, e)
      throw RetryableException(e)
    } catch (e: Exception) {
      addExceptionToTrace(e)
      log.error("Unable to create job for connection {}", input.connectionId, e)
      throw RetryableException(e)
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Throws(RetryableException::class)
  override fun createNewAttemptNumber(input: AttemptCreationInput): AttemptNumberCreationOutput {
    AttemptContext(null, input.jobId, null).addTagsToTrace()

    try {
      val jobId: Long = input.jobId!!
      val response = airbyteApiClient.attemptApi.createNewAttemptNumber(CreateNewAttemptNumberRequest(jobId))
      return AttemptNumberCreationOutput(response.attemptNumber)
    } catch (e: ClientException) {
      if (e.statusCode == HttpStatus.NOT_FOUND.getCode()) {
        throw e
      }
      log.error("createNewAttemptNumber for job {} failed with exception: {}", input.jobId, e.message, e)
      throw RetryableException(e)
    } catch (e: Exception) {
      addExceptionToTrace(e)
      log.error("createNewAttemptNumber for job {} failed with exception: {}", input.jobId, e.message, e)
      throw RetryableException(e)
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  override fun jobSuccessWithAttemptNumber(input: JobSuccessInputWithAttemptNumber) {
    AttemptContext(input.connectionId, input.jobId, input.attemptNumber).addTagsToTrace()

    val output = input.standardSyncOutput

    try {
      if (input.jobId != null) {
        val request =
          JobSuccessWithAttemptNumberRequest(
            input.jobId!!,
            input.attemptNumber!!,
            input.connectionId!!,
            output!!,
          )
        airbyteApiClient.jobsApi.jobSuccessWithAttemptNumber(request)
      } else {
        log.warn("Skipping job success update because job ID is null (connection ID = {}).", input.connectionId)
      }
    } catch (e: ClientException) {
      if (e.statusCode == HttpStatus.NOT_FOUND.getCode()) {
        throw e
      }
      throw RetryableException(e)
    } catch (e: IOException) {
      addExceptionToTrace(e)
      log.error("jobSuccessWithAttemptNumber for job {} failed with exception: {}", input.jobId, e.message, e)
      throw RetryableException(e)
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  override fun jobFailure(input: JobFailureInput) {
    AttemptContext(input.connectionId, input.jobId, input.attemptNumber).addTagsToTrace()

    try {
      if (input.jobId != null) {
        val request =
          JobFailureRequest(
            input.jobId!!,
            input.attemptNumber!!,
            input.connectionId!!,
            input.reason!!,
          )
        airbyteApiClient.jobsApi.jobFailure(request)
      } else {
        log.warn("Skipping job failure update because job ID is null (connection ID = {}).", input.connectionId)
      }
    } catch (e: ClientException) {
      if (e.statusCode == HttpStatus.NOT_FOUND.getCode()) {
        throw e
      }
      throw RetryableException(e)
    } catch (e: IOException) {
      log.error("jobFailure for job {} attempt {} failed with exception: {}", input.jobId, input.attemptNumber, e.message, e)
      throw RetryableException(e)
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  override fun attemptFailureWithAttemptNumber(input: AttemptNumberFailureInput) {
    AttemptContext(input.connectionId, input.jobId, input.attemptNumber).addTagsToTrace()

    val output = input.standardSyncOutput

    try {
      if (input.jobId != null) {
        val req =
          FailAttemptRequest(
            input.jobId!!,
            input.attemptNumber!!,
            input.attemptFailureSummary,
            output,
          )

        airbyteApiClient.attemptApi.failAttempt(req)
      } else {
        log.warn("Skipping attempt failure update because job ID is null (connection ID = {}).", input.connectionId)
      }
    } catch (e: ClientException) {
      if (e.statusCode == HttpStatus.NOT_FOUND.getCode()) {
        throw e
      }
      throw RetryableException(e)
    } catch (e: IOException) {
      log.error("attemptFailureWithAttemptNumber for job {} failed with exception: {}", input.jobId, e.message, e)
      throw RetryableException(e)
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  override fun jobCancelledWithAttemptNumber(input: JobCancelledInputWithAttemptNumber) {
    AttemptContext(input.connectionId, input.jobId, input.attemptNumber).addTagsToTrace()

    try {
      val req =
        PersistCancelJobRequestBody(
          input.attemptFailureSummary!!,
          input.attemptNumber!!,
          input.connectionId!!,
          input.jobId!!,
        )

      airbyteApiClient.jobsApi.persistJobCancellation(req)
    } catch (e: ClientException) {
      if (e.statusCode == HttpStatus.NOT_FOUND.getCode()) {
        throw e
      }
      throw RetryableException(e)
    } catch (e: IOException) {
      throw RetryableException(e)
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  override fun reportJobStart(input: ReportJobStartInput) {
    AttemptContext(input.connectionId, input.jobId, null).addTagsToTrace()

    try {
      airbyteApiClient.jobsApi.reportJobStart(ReportJobStartRequest(input.jobId!!, input.connectionId!!))
    } catch (e: ClientException) {
      if (e.statusCode == HttpStatus.NOT_FOUND.getCode()) {
        throw e
      }
      throw RetryableException(e)
    } catch (e: IOException) {
      throw RetryableException(e)
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  override fun ensureCleanJobState(input: EnsureCleanJobStateInput) {
    AttemptContext(input.connectionId, null, null).addTagsToTrace()
    try {
      airbyteApiClient.jobsApi.failNonTerminalJobs(ConnectionIdRequestBody(input.connectionId!!))
    } catch (e: ClientException) {
      if (e.statusCode == HttpStatus.NOT_FOUND.getCode()) {
        throw e
      }
      throw RetryableException(e)
    } catch (e: IOException) {
      throw RetryableException(e)
    }
  }

  /**
   * This method is used to determine if the current job is the last job or attempt failure.
   *
   * @param input - JobCheckFailureInput.
   * @return - boolean.
   */
  override fun isLastJobOrAttemptFailure(input: JobCheckFailureInput): Boolean {
    // This is a hack to enforce check operation before every sync. Please be mindful of this logic.
    // This is mainly for testing and to force our canary connections to always run CHECK
    if (shouldAlwaysRunCheckBeforeSync(input.connectionId!!)) {
      return true
    }
    // If there has been a previous attempt, that means it failed. We don't create subsequent attempts
    // on success.
    val isNotFirstAttempt = input.attemptId!! > 0
    if (isNotFirstAttempt) {
      return true
    }

    try {
      val didSucceed =
        airbyteApiClient.jobsApi
          .didPreviousJobSucceed(
            ConnectionJobRequestBody(input.connectionId!!, input.jobId!!),
          ).value
      // Treat anything other than an explicit success as a failure.
      return !didSucceed
    } catch (e: ClientException) {
      if (e.statusCode == HttpStatus.NOT_FOUND.getCode()) {
        throw e
      }
      throw RetryableException(e)
    } catch (e: IOException) {
      throw RetryableException(e)
    }
  }

  private fun shouldAlwaysRunCheckBeforeSync(connectionId: UUID): Boolean {
    try {
      return featureFlagClient.boolVariation(
        AlwaysRunCheckBeforeSync,
        Connection(connectionId),
      )
    } catch (e: Exception) {
      return false
    }
  }

  companion object {
    private val log: Logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())
  }
}
