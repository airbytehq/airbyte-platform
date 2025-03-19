/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.asyncProfiler

import dev.failsafe.RetryPolicy

interface FailSafeRetryPolicies {
  fun pidRetryPolicy(mainClassKeyword: String): RetryPolicy<Int?>

  fun fileDownloadRetryPolicy(url: String): RetryPolicy<Any?>
}
