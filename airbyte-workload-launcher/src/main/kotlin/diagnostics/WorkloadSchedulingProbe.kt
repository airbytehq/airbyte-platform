/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.diagnostics

import io.airbyte.api.client.model.generated.DataplaneDiagnosticKind
import io.airbyte.api.client.model.generated.DataplaneDiagnosticResult
import io.airbyte.api.client.model.generated.DataplaneDiagnosticStatus
import io.airbyte.micronaut.runtime.WORKLOAD_LAUNCHER_WORKLOAD_SCHEDULING_PROBE_RATE
import io.fabric8.kubernetes.client.KubernetesClientException
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Singleton
import java.time.Clock
import java.time.Instant
import java.util.UUID

private val logger = KotlinLogging.logger {}

/**
 * Checks whether workload pods are waiting for a node, every minute by default, and keeps the last successful scheduling
 * and the last problem between scans. Those are kept in memory only: after a restart the first scan reports only what the
 * pods still show.
 */
@Singleton
class WorkloadSchedulingProbe(
  private val workloadPods: WorkloadPodLister,
  private val clock: Clock,
) : ScheduledDiagnosticProbe() {
  // Only the scan thread reads or writes it.
  private var state = WorkloadSchedulingState()

  // The initial delay gives the launcher time to learn its data plane.
  @Scheduled(fixedRate = "\${$WORKLOAD_LAUNCHER_WORKLOAD_SCHEDULING_PROBE_RATE}", initialDelay = "1m", scheduler = "workload-scheduling-probe")
  fun scan() {
    val dataplane = dataplane ?: return

    val result =
      try {
        val pods = workloadPods.list()
        val scannedAt = clock.instant()
        state = state.after(scanPods(pods, scannedAt), scannedAt)
        completedResult(dataplane.dataplaneId, scannedAt)
      } catch (e: KubernetesClientException) {
        logger.warn(e) { "Could not list workload pods to check scheduling" }
        incompleteResult(dataplane.dataplaneId, clock.instant(), e)
      }
    publish(listOf(result))
  }

  private fun completedResult(
    dataplaneId: UUID,
    scannedAt: Instant,
  ): DataplaneDiagnosticResult {
    val blockedCount = state.blockedPods.size
    val observation = countsObservation() + eventsObservation()
    if (blockedCount == 0) {
      return result(dataplaneId, DataplaneDiagnosticStatus.PASSED, scannedAt, observation)
    }

    val workloads = if (blockedCount == 1) "1 workload pod has" else "$blockedCount workload pods have"
    val waited = "$workloads waited more than ${SCHEDULING_GRACE_PERIOD.toMinutes()} minutes for a node"
    val schedulerReason = state.longestBlocked?.message.orEmpty()
    return result(
      dataplaneId,
      DataplaneDiagnosticStatus.FAILED,
      scannedAt,
      observation,
      errorCode = UNSCHEDULABLE,
      errorMessage = if (schedulerReason.isBlank()) "$waited." else "$waited: $schedulerReason",
    )
  }

  /**
   * Leaves the counts out, since nothing was measured, and keeps the events of earlier scans, which carry their own times.
   */
  private fun incompleteResult(
    dataplaneId: UUID,
    attemptedAt: Instant,
    e: KubernetesClientException,
  ): DataplaneDiagnosticResult {
    val (errorCode, errorMessage) =
      when (e.code) {
        401, 403 -> ACCESS_DENIED to "The launcher is not allowed to list pods in its jobs namespace."
        else -> POD_LIST_FAILED to "The launcher could not list pods in its jobs namespace."
      }
    return result(dataplaneId, DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE, attemptedAt, eventsObservation(), errorCode, errorMessage)
  }

  private fun result(
    dataplaneId: UUID,
    status: DataplaneDiagnosticStatus,
    checkedAt: Instant,
    observation: Map<String, Any>,
    errorCode: String? = null,
    errorMessage: String? = null,
  ) = DataplaneDiagnosticResult(
    kind = DataplaneDiagnosticKind.WORKLOAD_SCHEDULING.value,
    targetId = dataplaneId,
    status = status,
    checkedAt = checkedAt.toEpochMilli(),
    errorCode = errorCode,
    errorMessage = errorMessage,
    observation = observation,
  )

  // Times are epoch milliseconds. A value not observed yet is left out, because the generated client's map can't hold null.
  private fun countsObservation(): Map<String, Any> =
    buildMap {
      put("blocked_count", state.blockedPods.size)
      state.longestBlocked?.let {
        put("longest_blocked_since", it.blockedSince.toEpochMilli())
        put("longest_blocked_workload_type", it.workloadType)
      }
    }

  private fun eventsObservation(): Map<String, Any> =
    buildMap {
      state.lastScheduled?.let {
        put("last_scheduled", mapOf("scheduled_at" to it.scheduledAt.toEpochMilli(), "workload_type" to it.workloadType))
      }
      state.retainedProblem?.let {
        put("last_problem", mapOf("observed_at" to it.observedAt.toEpochMilli(), "reason" to it.reason, "message" to it.message))
      }
    }

  companion object {
    const val UNSCHEDULABLE = "UNSCHEDULABLE"
    const val ACCESS_DENIED = "ACCESS_DENIED"
    const val POD_LIST_FAILED = "POD_LIST_FAILED"
  }
}
