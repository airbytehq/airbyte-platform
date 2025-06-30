/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal

import dev.failsafe.Failsafe
import dev.failsafe.RetryPolicy
import dev.failsafe.event.ExecutionAttemptedEvent
import dev.failsafe.function.CheckedSupplier
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Duration

/**
 * Helper class to provide consistent retry strategy across the temporal wrapper classes.
 */
class RetryHelper(
  private val metricClient: MetricClient,
  private val maxAttempt: Int,
  private val backoffDelayInMillis: Int,
  private val backoffMaxDelayInMillis: Int,
) {
  /**
   * Where the magic happens.
   *
   *
   * We should only retry errors that are transient GRPC network errors.
   *
   *
   * We should only retry idempotent calls. The caller should be responsible for retrying creates to
   * avoid generating additional noise.
   */
  fun <T> withRetries(
    call: CheckedSupplier<T>?,
    name: String,
  ): T {
    val retry =
      RetryPolicy
        .builder<Any>()
        .handleIf { t: Throwable -> this.shouldRetry(t) }
        .withMaxAttempts(maxAttempt)
        .withBackoff(Duration.ofMillis(backoffDelayInMillis.toLong()), Duration.ofMillis(backoffMaxDelayInMillis.toLong()))
        .onRetry { a: ExecutionAttemptedEvent<Any> ->
          metricClient.count(
            OssMetricsRegistry.TEMPORAL_API_TRANSIENT_ERROR_RETRY,
            1L,
            MetricAttribute(MetricTags.ATTEMPT_NUMBER, a.attemptCount.toString()),
            MetricAttribute(MetricTags.FAILURE_ORIGIN, name),
            MetricAttribute(MetricTags.FAILURE_TYPE, a.lastException.javaClass.name),
          )
        }.build()
    return Failsafe.with(retry).get(call)
  }

  private fun shouldRetry(t: Throwable): Boolean {
    // We are retrying Status.UNAVAILABLE because it is often sign of an unexpected connection
    // termination.
    return t is StatusRuntimeException && Status.UNAVAILABLE == t.status
  }

  companion object {
    const val DEFAULT_MAX_ATTEMPT: Int = 3
    const val DEFAULT_BACKOFF_DELAY_IN_MILLIS: Int = 1000
    const val DEFAULT_BACKOFF_MAX_DELAY_IN_MILLIS: Int = 10000
  }
}
