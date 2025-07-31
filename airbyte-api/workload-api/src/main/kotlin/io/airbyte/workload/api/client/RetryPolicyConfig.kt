/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.client

import io.airbyte.api.client.ApiException
import java.io.IOException
import java.lang.Exception
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * RetryPolicyConfig configures the retry policy
 *
 * TODO(cole): should this be replaced with an okhttp interceptor?
 */
data class RetryPolicyConfig(
  val delay: Duration = 2.seconds,
  val maxDelay: Duration? = null,
  val maxRetries: Int = 5,
  val jitterFactor: Double = 0.25,
  val exceptions: List<Class<out Exception>> =
    listOf(
      ApiException::class.java,
      IllegalArgumentException::class.java,
      IOException::class.java,
      UnsupportedOperationException::class.java,
    ),
)
