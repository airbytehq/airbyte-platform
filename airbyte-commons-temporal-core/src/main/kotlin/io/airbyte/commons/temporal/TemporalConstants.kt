/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal

import io.temporal.common.RetryOptions
import java.time.Duration

object TemporalConstants {
  @JvmField
  val NO_RETRY: RetryOptions = RetryOptions.newBuilder().setMaximumAttempts(1).build()
  val SEND_HEARTBEAT_INTERVAL: Duration = Duration.ofSeconds(20)

  @JvmField
  val HEARTBEAT_TIMEOUT: Duration = Duration.ofSeconds(30)
  val HEARTBEAT_SHUTDOWN_GRACE_PERIOD: Duration = Duration.ofSeconds(30)
}
