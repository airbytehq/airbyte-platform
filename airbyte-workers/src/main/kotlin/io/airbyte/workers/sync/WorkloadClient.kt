/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.sync

import io.airbyte.api.client.ApiException
import io.airbyte.commons.temporal.HeartbeatUtils
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.workers.workload.WorkloadConstants.WORKLOAD_CANCELLED_BY_USER_REASON
import io.airbyte.workers.workload.WorkloadOutputWriter
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.domain.Workload
import io.airbyte.workload.api.domain.WorkloadCancelRequest
import io.airbyte.workload.api.domain.WorkloadCreateRequest
import io.airbyte.workload.api.domain.WorkloadStatus
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.HttpStatus
import io.temporal.activity.ActivityExecutionContext
import jakarta.inject.Singleton
import java.io.IOException
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration.Companion.seconds

private val logger = KotlinLogging.logger { }

/**
 * WorkloadClient that abstracts common interactions with the workload-api.
 * This client should be preferred over direct usage of the WorkloadApiClient.
 */
@Singleton
class WorkloadClient(
  private val workloadApiClient: WorkloadApiClient,
  private val outputWriter: WorkloadOutputWriter,
) {
  companion object {
    const val CANCELLATION_SOURCE_STR = "Cancellation callback."
    val TERMINAL_STATUSES = setOf(WorkloadStatus.SUCCESS, WorkloadStatus.FAILURE, WorkloadStatus.CANCELLED)
  }

  fun createWorkload(workloadCreateRequest: WorkloadCreateRequest) {
    try {
      workloadApiClient.workloadCreate(workloadCreateRequest)
    } catch (e: ApiException) {
      /*
       * The Workload API returns a 409 response when the request to execute the workload has already been
       * created. That response is handled in the form of an ApiException by the retrofit WorkloadApiClient.
       * We do not want to cause the Temporal workflow to retry, so catch it and log the information so that the workflow will continue.
       */
      if (e.statusCode == HttpStatus.CONFLICT.code) {
        logger.warn { "Workload ${workloadCreateRequest.workloadId} already created and in progress.  Continuing..." }
      } else {
        throw RuntimeException(e)
      }
    } catch (e: IOException) {
      throw RuntimeException(e)
    }
  }

  fun isTerminal(workloadId: String) = isWorkloadTerminal(workloadApiClient.workloadGet(workloadId))

  fun waitForWorkload(
    workloadId: String,
    pollingFrequencyInSeconds: Int,
  ) {
    try {
      var workload = workloadApiClient.workloadGet(workloadId)
      while (!isWorkloadTerminal(workload)) {
        Thread.sleep(pollingFrequencyInSeconds.seconds.inWholeMilliseconds)
        workload = workloadApiClient.workloadGet(workloadId)
      }
    } catch (e: IOException) {
      throw RuntimeException(e)
    } catch (e: InterruptedException) {
      throw RuntimeException(e)
    }
  }

  fun getConnectorJobOutput(
    workloadId: String,
    onFailure: (FailureReason) -> ConnectorJobOutput,
  ): ConnectorJobOutput =
    Result
      .runCatching {
        outputWriter.read(workloadId).orElseThrow()
      }.fold(
        onFailure = { t -> onFailure(handleMissingConnectorJobOutput(workloadId, t)) },
        onSuccess = { x -> x },
      )

  /**
   * Attempts to cancel the workload and swallows errors if it fails.
   */
  fun cancelWorkloadBestEffort(req: WorkloadCancelRequest) {
    try {
      workloadApiClient.workloadCancel(req)
    } catch (e: ApiException) {
      when (e.statusCode) {
        HttpStatus.GONE.code -> logger.warn { "Workload: ${req.workloadId} already terminal. Cancellation is a no-op." }
        HttpStatus.NOT_FOUND.code -> logger.warn { "Workload: ${req.workloadId} not yet created. Cancellation is a no-op." }
        else -> logger.error(e) { "Workload: ${req.workloadId} failed to be cancelled due to API error. Status code: ${e.statusCode}" }
      }
    } catch (e: Exception) {
      logger.error(e) { "Workload: ${req.workloadId} failed to be cancelled." }
    }
  }

  /**
   * Creates and waits for workload propagating any Temporal level cancellations (detected by the heartbeats) to the workload.
   */
  fun runWorkloadWithCancellationHeartbeat(
    createReq: WorkloadCreateRequest,
    checkFrequencyInSeconds: Int,
    context: ActivityExecutionContext,
  ) {
    val cancellationCallback =
      Runnable {
        val failReq = WorkloadCancelRequest(createReq.workloadId, WORKLOAD_CANCELLED_BY_USER_REASON, CANCELLATION_SOURCE_STR)
        cancelWorkloadBestEffort(failReq)
      }
    HeartbeatUtils.withBackgroundHeartbeat(
      AtomicReference(cancellationCallback),
      {
        createWorkload(createReq)
        waitForWorkload(createReq.workloadId, checkFrequencyInSeconds)
      },
      context,
    )
  }

  private fun handleMissingConnectorJobOutput(
    workloadId: String,
    t: Throwable?,
  ): FailureReason {
    return Result
      .runCatching {
        val workload = workloadApiClient.workloadGet(workloadId)

        return when (workload.status) {
          // This is pretty bad, the workload succeeded, but we failed to read the output
          WorkloadStatus.SUCCESS ->
            FailureReason()
              .withFailureOrigin(FailureReason.FailureOrigin.AIRBYTE_PLATFORM)
              .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)
              .withExternalMessage("Failed to read the output")
              .withInternalMessage("Failed to read the output of a successful workload $workloadId")
              .withStacktrace(t?.stackTraceToString())

          // do some classification from workload.terminationSource
          WorkloadStatus.CANCELLED, WorkloadStatus.FAILURE ->
            FailureReason()
              .withFailureOrigin(
                when (workload.terminationSource) {
                  "source" -> FailureReason.FailureOrigin.SOURCE
                  "destination" -> FailureReason.FailureOrigin.DESTINATION
                  else -> FailureReason.FailureOrigin.AIRBYTE_PLATFORM
                },
              ).withExternalMessage(
                "Workload ${if (workload.status == WorkloadStatus.CANCELLED) "cancelled by" else "failed, source:"} ${workload.terminationSource}",
              ).withInternalMessage(workload.terminationReason)

          // We should never be in this situation, workload is still running not having an output is expected,
          // we should not be trying to read the output of a non-terminal workload.
          else ->
            FailureReason()
              .withFailureOrigin(FailureReason.FailureOrigin.AIRBYTE_PLATFORM)
              .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)
              .withExternalMessage("Expected error in the platform")
              .withInternalMessage("$workloadId isn't in a terminal state, no output available")
        }
      }.getOrElse {
        FailureReason()
          .withFailureOrigin(FailureReason.FailureOrigin.AIRBYTE_PLATFORM)
          .withFailureType(FailureReason.FailureType.TRANSIENT_ERROR)
          .withExternalMessage("Platform failure")
          .withInternalMessage("Unable to reach the workload-api")
          .withStacktrace(it.stackTraceToString())
      }
  }

  private fun isWorkloadTerminal(workload: Workload): Boolean = workload.status in TERMINAL_STATUSES
}
