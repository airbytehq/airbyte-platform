/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.runtime

import io.micronaut.context.annotation.ConfigurationProperties

internal const val DEFAULT_WORKER_ACTIVITY_INITIAL_DELAY_SECONDS = 30
internal const val DEFAULT_WORKER_ACTIVITY_MAX_ATTEMPTS = 5
internal const val DEFAULT_WORKER_ACTIVITY_MAX_DELAY_SECONDS = 600
internal const val DEFAULT_WORKER_ACTIVITY_MAX_TIMEOUT_SECONDS = 120L
internal const val DEFAULT_WORKER_ACTIVITY_CHECK_TIMEOUT_SECONDS = 10L
internal const val DEFAULT_WORKER_ACTIVITY_DISCOVERY_TIMEOUT_SECONDS = 30L

@ConfigurationProperties("airbyte.activity")
data class AirbyteWorkerActivityConfig(
  val initialDelay: Int = DEFAULT_WORKER_ACTIVITY_INITIAL_DELAY_SECONDS,
  val maxAttempts: Int = DEFAULT_WORKER_ACTIVITY_MAX_ATTEMPTS,
  val maxDelay: Int = DEFAULT_WORKER_ACTIVITY_MAX_DELAY_SECONDS,
  val maxTimeout: Long = DEFAULT_WORKER_ACTIVITY_MAX_TIMEOUT_SECONDS,
  val checkTimeout: Long = DEFAULT_WORKER_ACTIVITY_CHECK_TIMEOUT_SECONDS,
  val discoveryTimeout: Long = DEFAULT_WORKER_ACTIVITY_DISCOVERY_TIMEOUT_SECONDS,
)
