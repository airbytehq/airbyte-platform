/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.core

import io.airbyte.config.FailureReason
import io.airbyte.config.WorkloadType
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.domain.WorkloadFailureRequest
import io.airbyte.workload.api.domain.WorkloadRunningRequest
import io.airbyte.workload.api.domain.WorkloadStatus
import io.airbyte.workload.api.domain.WorkloadSuccessRequest
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.OffsetDateTime
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService

private val logger = KotlinLogging.logger {}

private val ELIGIBLE_STATUSES = setOf<WorkloadStatus?>(WorkloadStatus.CLAIMED, WorkloadStatus.LAUNCHED)

/** Name of the executor direct jobs run on, configured under `micronaut.executors`. */
const val DIRECT_JOB_EXECUTOR = "direct-jobs"

sealed interface DirectJobSubmission {
  data object Accepted : DirectJobSubmission

  /** The workload is already running on this runner. */
  data object Duplicate : DirectJobSubmission

  /** The workload is not CLAIMED or LAUNCHED, so it must not be (re)run. */
  data class NotEligible(
    val status: WorkloadStatus?,
  ) : DirectJobSubmission

  /** No handler is registered for the workload's type. */
  data class Unsupported(
    val type: WorkloadType,
  ) : DirectJobSubmission
}

/**
 * Accepts direct jobs and runs them with the [DirectJobHandler] registered for the workload's type.
 *
 * The checks here apply to every direct job, whatever its type:
 * - a workload already in flight on this runner is a [DirectJobSubmission.Duplicate];
 * - only CLAIMED or LAUNCHED workloads start, so cancelled, finished, or re-delivered workloads are not rerun;
 * - the workload is marked RUNNING before the submission is acknowledged, so a re-delivery to any runner sees it.
 */
@Singleton
class DirectJobService(
  private val workloadApiClient: WorkloadApiClient,
  @Named(DIRECT_JOB_EXECUTOR) private val executor: ExecutorService,
  handlers: List<DirectJobHandler>,
) {
  private val handlersByType: Map<WorkloadType, DirectJobHandler> =
    handlers.groupBy { it.workloadType }.mapValues { (type, registered) ->
      require(registered.size == 1) { "Expected one direct job handler for $type, found ${registered.size}" }
      registered.single()
    }

  private val inFlight: MutableSet<String> = ConcurrentHashMap.newKeySet()

  /**
   * Validates the workload and marks it running synchronously, then runs the job in the background. The workload
   * API is the source of truth for the result; callers only learn whether the work was taken.
   */
  fun submit(workloadId: String): DirectJobSubmission {
    if (!inFlight.add(workloadId)) {
      return DirectJobSubmission.Duplicate
    }

    var handedOff = false
    try {
      val workload = workloadApiClient.workloadGet(workloadId)

      val handler = handlersByType[workload.type] ?: return DirectJobSubmission.Unsupported(workload.type)

      if (workload.status !in ELIGIBLE_STATUSES) {
        return DirectJobSubmission.NotEligible(workload.status)
      }

      // Mark the workload running before acknowledging, so a re-delivery to any runner sees RUNNING and is rejected.
      workloadApiClient.workloadRunning(WorkloadRunningRequest(workloadId, OffsetDateTime.now().plus(handler.runningDeadline)))

      val inputPayload = workload.inputPayload
      executor.execute { run(handler, workloadId, inputPayload) }
      handedOff = true
      return DirectJobSubmission.Accepted
    } finally {
      if (!handedOff) {
        inFlight.remove(workloadId)
      }
    }
  }

  private fun run(
    handler: DirectJobHandler,
    workloadId: String,
    inputPayload: String,
  ) {
    try {
      handler.run(workloadId, inputPayload)
      workloadApiClient.workloadSuccess(WorkloadSuccessRequest(workloadId))
    } catch (e: Exception) {
      logger.error(e) { "Direct job ${handler.workloadType} for workload $workloadId failed unexpectedly" }
      failWorkload(handler.workloadType, workloadId, e)
    } finally {
      inFlight.remove(workloadId)
    }
  }

  // This runs on a background thread with nobody to propagate to, so a failure to report the failure is only logged.
  private fun failWorkload(
    type: WorkloadType,
    workloadId: String,
    cause: Exception,
  ) {
    try {
      workloadApiClient.workloadFailure(
        WorkloadFailureRequest(
          workloadId,
          FailureReason.FailureOrigin.AIRBYTE_PLATFORM.value(),
          "The $type job failed unexpectedly: ${cause.message ?: cause.javaClass.simpleName}",
        ),
      )
    } catch (e: Exception) {
      logger.error(e) { "Unable to mark workload $workloadId as failed" }
    }
  }
}
