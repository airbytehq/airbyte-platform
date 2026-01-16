/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.runtime

import io.micronaut.context.annotation.ConfigurationProperties

internal const val DEFAULT_WORKER_MAX_SUCCESSIVE_COMPLETE_FAILURES = 5
internal const val DEFAULT_WORKER_MAX_TOTAL_COMPLETE_FAILURES = 10
internal const val DEFAULT_WORKER_COMPLETE_FAILURES_MIN_INTERVAL_SECONDS = 10
internal const val DEFAULT_WORKER_COMPLETE_FAILURES_MAX_INTERVAL_SECONDS = 1800
internal const val DEFAULT_WORKER_COMPLETE_FAILURES_BASE_INTERVAL_SECONDS = 3
internal const val DEFAULT_WORKER_MAX_SUCCESSIVE_PARTIAL_FAILURES = 1000
internal const val DEFAULT_WORKER_MAX_TOTAL_PARTIAL_FAILURES = 20

@ConfigurationProperties("airbyte.retries")
data class AirbyteWorkerRetryConfig(
  val completeFailures: AirbyteWorkerCompleteFailuresRetryConfig = AirbyteWorkerCompleteFailuresRetryConfig(),
  val partialFailures: AirbyteWorkerPartialFailuresRetryConfig = AirbyteWorkerPartialFailuresRetryConfig(),
) {
  @ConfigurationProperties("complete-failures")
  data class AirbyteWorkerCompleteFailuresRetryConfig(
    val maxSuccessive: Int = DEFAULT_WORKER_MAX_SUCCESSIVE_COMPLETE_FAILURES,
    val maxTotal: Int = DEFAULT_WORKER_MAX_TOTAL_COMPLETE_FAILURES,
    val backoff: AirbyteWorkerCompleteFailuresBackoffRetryConfig = AirbyteWorkerCompleteFailuresBackoffRetryConfig(),
  ) {
    @ConfigurationProperties("backoff")
    data class AirbyteWorkerCompleteFailuresBackoffRetryConfig(
      val minIntervalS: Int = DEFAULT_WORKER_COMPLETE_FAILURES_MIN_INTERVAL_SECONDS,
      val maxIntervalS: Int = DEFAULT_WORKER_COMPLETE_FAILURES_MAX_INTERVAL_SECONDS,
      val base: Int = DEFAULT_WORKER_COMPLETE_FAILURES_BASE_INTERVAL_SECONDS,
    )
  }

  @ConfigurationProperties("partial-failures")
  data class AirbyteWorkerPartialFailuresRetryConfig(
    val maxSuccessive: Int = DEFAULT_WORKER_MAX_SUCCESSIVE_PARTIAL_FAILURES,
    val maxTotal: Int = DEFAULT_WORKER_MAX_TOTAL_PARTIAL_FAILURES,
  )
}
