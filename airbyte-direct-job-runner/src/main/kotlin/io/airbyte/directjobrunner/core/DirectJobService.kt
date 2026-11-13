/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.core

import io.airbyte.config.FailureReason
import io.airbyte.config.WorkloadType
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.domain.WorkloadFailureRequest
import io.airbyte.workload.api.domain.WorkloadHeartbeatRequest
import io.airbyte.workload.api.domain.WorkloadRunningRequest
import io.airbyte.workload.api.domain.WorkloadStatus
import io.airbyte.workload.api.domain.WorkloadSuccessRequest
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.annotation.PreDestroy
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.Duration
import java.time.OffsetDateTime
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.function.ToDoubleFunction

private val logger = KotlinLogging.logger {}

private val ELIGIBLE_STATUSES = setOf<WorkloadStatus?>(WorkloadStatus.CLAIMED, WorkloadStatus.LAUNCHED)

/** Name of the executor CONNECT jobs run on, configured under `micronaut.executors`. */
const val CONNECT_EXECUTOR = "connect"

/** Name of the executor checks run on. Its own pool, so connector JVMs are bounded and never hold up CONNECT jobs. */
const val DIRECT_CHECK_EXECUTOR = "direct-checks"

private const val RUN_SUCCEEDED = "success"
private const val RUN_FAILED = "failure"

/** The job expired or was cancelled while it waited for a thread, so it was not run. */
private const val RUN_EXPIRED = "expired"

private val DRAIN_POLL: Duration = Duration.ofMillis(200)

sealed interface DirectJobSubmission {
  data object Accepted : DirectJobSubmission

  /** The workload is already running on this runner. */
  data object Duplicate : DirectJobSubmission

  /** The workload is not CLAIMED or LAUNCHED, so it must not be (re)run. */
  data class NotEligible(
    val status: WorkloadStatus?,
  ) : DirectJobSubmission

  /** No handler is registered for the workload's type, or the handler can't run this workload. */
  data class Unsupported(
    val type: WorkloadType,
  ) : DirectJobSubmission

  /** The runner is shutting down and only finishes the jobs it already has. */
  data object ShuttingDown : DirectJobSubmission
}

/**
 * Accepts direct jobs and runs them with the [DirectJobHandler] registered for the workload's type.
 *
 * The checks here apply to every direct job, whatever its type:
 * - a workload already in flight on this runner is a [DirectJobSubmission.Duplicate];
 * - a workload its handler can't run ([DirectJobHandler.accepts]) is [DirectJobSubmission.Unsupported];
 * - only CLAIMED or LAUNCHED workloads start, so cancelled, finished, or re-delivered workloads are not rerun;
 * - the workload is marked RUNNING before the submission is acknowledged, so a re-delivery to any runner sees it.
 */
@Singleton
class DirectJobService(
  private val workloadApiClient: WorkloadApiClient,
  @Named(CONNECT_EXECUTOR) private val executor: ExecutorService,
  @Named(DIRECT_CHECK_EXECUTOR) private val checkExecutor: ExecutorService,
  handlers: List<DirectJobHandler>,
  private val metricClient: MetricClient,
  // How long shutdown waits for running jobs to finish (see drain). Kubernetes kills the pod terminationGracePeriodSeconds
  // after it asks it to stop, so this must be shorter than that or the jobs it waits for are killed anyway.
  @Value("\${airbyte.direct-job-runner.drain-timeout:PT80S}") private val drainTimeout: Duration = Duration.ofSeconds(80),
) {
  private val handlersByType: Map<WorkloadType, DirectJobHandler> =
    handlers.groupBy { it.workloadType }.mapValues { (type, registered) ->
      require(registered.size == 1) { "Expected one direct job handler for $type, found ${registered.size}" }
      registered.single()
    }

  private val inFlight: MutableSet<String> =
    ConcurrentHashMap.newKeySet<String>().also { inFlight ->
      metricClient.gauge(OssMetricsRegistry.DIRECT_JOB_IN_FLIGHT, inFlight, ToDoubleFunction { it.size.toDouble() })
    }

  @Volatile
  private var shuttingDown = false

  /**
   * Validates the workload and marks it running synchronously, then runs the job in the background. The workload
   * API is the source of truth for the result; callers only learn whether the work was taken.
   */
  fun submit(
    workloadId: String,
    dataplaneGroup: LauncherDataplaneGroup,
  ): DirectJobSubmission {
    if (shuttingDown) {
      return recordSubmission(MetricTags.UNKNOWN, DirectJobSubmission.ShuttingDown)
    }
    if (!inFlight.add(workloadId)) {
      // The workload has not been read yet, so its type is unknown.
      return recordSubmission(MetricTags.UNKNOWN, DirectJobSubmission.Duplicate)
    }

    var handedOff = false
    try {
      val workload = workloadApiClient.workloadGet(workloadId)

      val workloadType = workload.type.toString()
      val handler =
        handlersByType[workload.type] ?: return recordSubmission(workloadType, DirectJobSubmission.Unsupported(workload.type))
      if (!handler.accepts(workload, dataplaneGroup)) {
        return recordSubmission(workloadType, DirectJobSubmission.Unsupported(workload.type))
      }

      if (workload.status !in ELIGIBLE_STATUSES) {
        return recordSubmission(workloadType, DirectJobSubmission.NotEligible(workload.status))
      }

      // Mark the workload running before acknowledging, so a re-delivery to any runner sees RUNNING and is rejected.
      workloadApiClient.workloadRunning(WorkloadRunningRequest(workloadId, OffsetDateTime.now().plus(handler.runningDeadline)))

      val inputPayload = workload.inputPayload
      val logPath = workload.logPath
      val pool = if (workload.type == WorkloadType.CHECK) checkExecutor else executor
      val queuedNanos = System.nanoTime()
      pool.execute { run(handler, workloadId, inputPayload, logPath, queuedNanos) }
      handedOff = true
      return recordSubmission(workloadType, DirectJobSubmission.Accepted)
    } finally {
      if (!handedOff) {
        inFlight.remove(workloadId)
      }
    }
  }

  /**
   * Stops taking jobs and waits, up to [drainTimeout], for the running ones to report their results. Without this, a
   * deploy or node drain kills checks mid-run and leaves their workloads RUNNING until the deadline.
   */
  @PreDestroy
  fun drain() {
    shuttingDown = true
    val deadline = System.currentTimeMillis() + drainTimeout.toMillis()
    while (inFlight.isNotEmpty() && System.currentTimeMillis() < deadline) {
      Thread.sleep(DRAIN_POLL.toMillis())
    }
    if (inFlight.isNotEmpty()) {
      logger.warn { "Shutting down with ${inFlight.size} direct jobs still running: $inFlight" }
    }
  }

  private fun recordSubmission(
    workloadType: String,
    submission: DirectJobSubmission,
  ): DirectJobSubmission {
    val result =
      when (submission) {
        DirectJobSubmission.Accepted -> "accepted"
        DirectJobSubmission.Duplicate -> "duplicate"
        is DirectJobSubmission.NotEligible -> "not_eligible"
        is DirectJobSubmission.Unsupported -> "unsupported"
        DirectJobSubmission.ShuttingDown -> "shutting_down"
      }
    metricClient.count(
      OssMetricsRegistry.DIRECT_JOB_SUBMISSION,
      1,
      MetricAttribute(MetricTags.WORKLOAD_TYPE_TAG, workloadType),
      MetricAttribute(MetricTags.RESULT_TAG, result),
    )
    return submission
  }

  private fun run(
    handler: DirectJobHandler,
    workloadId: String,
    inputPayload: String,
    logPath: String,
    queuedNanos: Long,
  ) {
    val workloadType = MetricAttribute(MetricTags.WORKLOAD_TYPE_TAG, handler.workloadType.toString())
    metricClient.distribution(OssMetricsRegistry.DIRECT_JOB_QUEUE_WAIT, elapsedMs(queuedNanos), workloadType)

    val result =
      try {
        if (!restartDeadline(handler, workloadId)) {
          RUN_EXPIRED
        } else {
          runHandler(handler, workloadId, inputPayload, logPath, workloadType)
          workloadApiClient.workloadSuccess(WorkloadSuccessRequest(workloadId))
          RUN_SUCCEEDED
        }
      } catch (e: Exception) {
        logger.error(e) { "Direct job ${handler.workloadType} for workload $workloadId failed unexpectedly" }
        failWorkload(handler.workloadType, workloadId, e)
        RUN_FAILED
      } finally {
        inFlight.remove(workloadId)
      }

    metricClient.count(OssMetricsRegistry.DIRECT_JOB_RUN, 1, workloadType, MetricAttribute(MetricTags.RESULT_TAG, result))
  }

  /**
   * The deadline set at admission also covered the job's time in the queue, so it starts over when the job does.
   * Returns false when the workload is no longer RUNNING (it expired or was cancelled while queued), so it isn't run.
   */
  private fun restartDeadline(
    handler: DirectJobHandler,
    workloadId: String,
  ): Boolean {
    try {
      workloadApiClient.workloadHeartbeat(WorkloadHeartbeatRequest(workloadId, OffsetDateTime.now().plus(handler.runningDeadline)))
      return true
    } catch (e: Exception) {
      val status = runCatching { workloadApiClient.workloadGet(workloadId).status }.getOrNull()
      logger.warn(e) { "Could not restart the deadline of workload $workloadId (status $status)" }
      // Only skip a job known not to be RUNNING; if the status can't be read either, run it.
      return status == null || status == WorkloadStatus.RUNNING
    }
  }

  /**
   * Times the handler alone. Reporting the result to the workload API is excluded: its retries can add seconds that
   * have nothing to do with the job.
   */
  private fun runHandler(
    handler: DirectJobHandler,
    workloadId: String,
    inputPayload: String,
    logPath: String,
    workloadType: MetricAttribute,
  ) {
    val startNanos = System.nanoTime()
    var result = RUN_FAILED
    try {
      handler.run(workloadId, inputPayload, logPath)
      result = RUN_SUCCEEDED
    } finally {
      metricClient.distribution(
        OssMetricsRegistry.DIRECT_JOB_RUN_DURATION,
        elapsedMs(startNanos),
        workloadType,
        MetricAttribute(MetricTags.RESULT_TAG, result),
      )
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
      metricClient.count(OssMetricsRegistry.DIRECT_JOB_FAILURE_REPORT_ERROR, 1, MetricAttribute(MetricTags.WORKLOAD_TYPE_TAG, type.toString()))
    }
  }

  private fun elapsedMs(startNanos: Long): Double = Duration.ofNanos(System.nanoTime() - startNanos).toMillis().toDouble()
}
