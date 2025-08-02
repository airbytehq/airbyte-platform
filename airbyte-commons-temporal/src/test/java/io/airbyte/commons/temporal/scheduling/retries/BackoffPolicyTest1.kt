/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling.retries

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.Duration

internal class BackoffPolicyTest1 {
  @Test
  fun getBackoffLowBase() {
    val policy = BackoffPolicy(Duration.ofSeconds(10), Duration.ofHours(1), 2)

    Assertions.assertEquals(0, policy.getBackoff(0).toSeconds())
    Assertions.assertEquals(10, policy.getBackoff(1).toSeconds())
    Assertions.assertEquals(20, policy.getBackoff(2).toSeconds())
    Assertions.assertEquals(40, policy.getBackoff(3).toSeconds())
    Assertions.assertEquals(80, policy.getBackoff(4).toSeconds())
    Assertions.assertEquals(160, policy.getBackoff(5).toSeconds())
    Assertions.assertEquals(320, policy.getBackoff(6).toSeconds())
    Assertions.assertEquals(640, policy.getBackoff(7).toSeconds())
    Assertions.assertEquals(1280, policy.getBackoff(8).toSeconds())
    Assertions.assertEquals(2560, policy.getBackoff(9).toSeconds()) // 42 minutes
    Assertions.assertEquals(1, policy.getBackoff(10).toHours())
    Assertions.assertEquals(1, policy.getBackoff(11).toHours())
    Assertions.assertEquals(1, policy.getBackoff(12).toHours())
  }

  @Test
  fun getBackoffHighBase() {
    val policy = BackoffPolicy(Duration.ofSeconds(30), Duration.ofMinutes(30), 6)

    Assertions.assertEquals(0, policy.getBackoff(0).toSeconds())
    Assertions.assertEquals(30, policy.getBackoff(1).toSeconds())
    Assertions.assertEquals(180, policy.getBackoff(2).toSeconds())
    Assertions.assertEquals(1080, policy.getBackoff(3).toSeconds())
    Assertions.assertEquals(30, policy.getBackoff(4).toMinutes())
    Assertions.assertEquals(30, policy.getBackoff(5).toMinutes())
  }
}
