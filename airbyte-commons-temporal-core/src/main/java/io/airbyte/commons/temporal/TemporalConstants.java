/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal;

import io.temporal.common.RetryOptions;
import java.time.Duration;

public class TemporalConstants {

  public static final RetryOptions NO_RETRY = RetryOptions.newBuilder().setMaximumAttempts(1).build();
  public static final Duration SEND_HEARTBEAT_INTERVAL = Duration.ofSeconds(20);
  public static final Duration HEARTBEAT_TIMEOUT = Duration.ofSeconds(30);
  public static final Duration HEARTBEAT_SHUTDOWN_GRACE_PERIOD = Duration.ofSeconds(30);

}
