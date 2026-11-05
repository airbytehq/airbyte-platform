/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.dispatch

import dev.failsafe.Failsafe
import dev.failsafe.FailsafeException
import dev.failsafe.RetryPolicy
import dev.failsafe.function.CheckedSupplier
import io.airbyte.commons.json.Jsons
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.micronaut.runtime.AirbyteWorkloadLauncherConfig
import io.airbyte.workers.exception.WorkloadLauncherException
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import java.io.IOException
import java.time.Duration

private val logger = KotlinLogging.logger {}

internal const val DIRECT_JOB_RUNNER_HTTP_CLIENT = "directJobRunnerHttpClient"
internal const val DIRECT_JOB_RUNNER_UNAVAILABLE_MESSAGE = "Direct job runner unavailable in this data plane"

private const val ACCEPTED = 202
private const val CONFLICT = 409
private const val SERVER_ERROR_MIN = 500
private const val MAX_RETRIES = 3
private val RETRY_INITIAL_DELAY: Duration = Duration.ofMillis(100)
private val RETRY_MAX_DELAY: Duration = Duration.ofSeconds(1)
private val JSON_MEDIA_TYPE = "application/json".toMediaType()

/** What happened when a workload was offered to the direct job runner. */
sealed interface DirectJobDispatch {
  /** The runner took the workload, or already had it. */
  data object Accepted : DirectJobDispatch

  /** The runner did not take the workload; [reason] is the metric's result tag. */
  data class Rejected(
    val reason: String,
  ) : DirectJobDispatch
}

/**
 * Hands a claimed workload to the direct job runner instead of launching a pod. Only the workload ID is sent: the
 * runner reads the workload itself and picks the handler for its type, so this client is the same for every direct
 * job type.
 */
@Singleton
class DirectJobRunnerClient(
  @Named(DIRECT_JOB_RUNNER_HTTP_CLIENT) private val httpClient: OkHttpClient,
  launcherConfig: AirbyteWorkloadLauncherConfig,
  private val metricClient: MetricClient,
) {
  private val runnerUrl: String = launcherConfig.directJobRunner.url.trimEnd('/')

  // The runner queues work rather than rejecting it, so only connection errors and 5xx are worth retrying.
  private val retryPolicy: RetryPolicy<Int> =
    RetryPolicy
      .builder<Int>()
      .handle(IOException::class.java)
      .handleResultIf { it >= SERVER_ERROR_MIN }
      .withBackoff(RETRY_INITIAL_DELAY, RETRY_MAX_DELAY)
      .withMaxRetries(MAX_RETRIES)
      .build()

  /** Offers the workload to the runner and fails it when the runner does not take it. For jobs that have no pod. */
  fun submit(msg: LauncherInput) {
    val dispatchStatus = dispatch(msg)
    if (dispatchStatus is DirectJobDispatch.Rejected) {
      record(msg, dispatchStatus.reason)
      throw WorkloadLauncherException(DIRECT_JOB_RUNNER_UNAVAILABLE_MESSAGE)
    }
  }

  /**
   * Offers the workload to the runner and records the handoffs it took. A rejection is left to the caller to record,
   * because what happens next (a failure or a pod) is the caller's choice.
   */
  fun dispatch(msg: LauncherInput): DirectJobDispatch {
    if (runnerUrl.isBlank()) {
      logger.warn { "Cannot submit direct job ${msg.workloadId}: no direct job runner URL is configured" }
      return DirectJobDispatch.Rejected("not_configured")
    }

    val request =
      Request
        .Builder()
        .url("$runnerUrl/v1/jobs")
        .post(Jsons.serialize(mapOf("workloadId" to msg.workloadId)).toRequestBody(JSON_MEDIA_TYPE))
        .build()

    val status =
      try {
        Failsafe.with(retryPolicy).get(CheckedSupplier { httpClient.newCall(request).execute().use { it.code } })
      } catch (e: FailsafeException) {
        logger.warn(e) { "Could not reach the direct job runner for workload ${msg.workloadId}" }
        return DirectJobDispatch.Rejected("unreachable")
      }

    return when (status) {
      ACCEPTED -> accepted(msg, "accepted")
      CONFLICT -> {
        logger.info { "The direct job runner already has workload ${msg.workloadId}" }
        accepted(msg, "duplicate")
      }
      else -> {
        logger.warn { "The direct job runner rejected workload ${msg.workloadId} with status $status" }
        DirectJobDispatch.Rejected("rejected")
      }
    }
  }

  private fun accepted(
    msg: LauncherInput,
    result: String,
  ): DirectJobDispatch {
    record(msg, result)
    return DirectJobDispatch.Accepted
  }

  private fun record(
    msg: LauncherInput,
    result: String,
  ) = metricClient.count(
    OssMetricsRegistry.DIRECT_JOB_DISPATCH,
    1,
    MetricAttribute(MetricTags.WORKLOAD_TYPE_TAG, msg.workloadType.toString()),
    MetricAttribute(MetricTags.RESULT_TAG, result),
  )
}
