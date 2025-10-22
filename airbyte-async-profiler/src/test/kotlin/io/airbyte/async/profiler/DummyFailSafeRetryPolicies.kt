/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.async.profiler

import dev.failsafe.RetryPolicy

class DummyFailSafeRetryPolicies : FailSafeRetryPolicies {
  override fun pidRetryPolicy(mainClassKeyword: String): RetryPolicy<Int?> {
    // Return a simple policy that does not retry.
    return RetryPolicy
      .builder<Int?>()
      .withMaxRetries(0)
      .build()
  }

  override fun fileDownloadRetryPolicy(url: String): RetryPolicy<Any?> {
    // Return a simple policy that does not retry.
    return RetryPolicy
      .builder<Any?>()
      .withMaxRetries(0)
      .build()
  }
}
