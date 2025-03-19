/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling.retries;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class BackoffPolicyTest {

  @Test
  void getBackoffLowBase() {
    final var policy = new BackoffPolicy(Duration.ofSeconds(10), Duration.ofHours(1), 2);

    assertEquals(0, policy.getBackoff(0).toSeconds());
    assertEquals(10, policy.getBackoff(1).toSeconds());
    assertEquals(20, policy.getBackoff(2).toSeconds());
    assertEquals(40, policy.getBackoff(3).toSeconds());
    assertEquals(80, policy.getBackoff(4).toSeconds());
    assertEquals(160, policy.getBackoff(5).toSeconds());
    assertEquals(320, policy.getBackoff(6).toSeconds());
    assertEquals(640, policy.getBackoff(7).toSeconds());
    assertEquals(1280, policy.getBackoff(8).toSeconds());
    assertEquals(2560, policy.getBackoff(9).toSeconds()); // 42 minutes
    assertEquals(1, policy.getBackoff(10).toHours());
    assertEquals(1, policy.getBackoff(11).toHours());
    assertEquals(1, policy.getBackoff(12).toHours());
  }

  @Test
  void getBackoffHighBase() {
    final var policy = new BackoffPolicy(Duration.ofSeconds(30), Duration.ofMinutes(30), 6);

    assertEquals(0, policy.getBackoff(0).toSeconds());
    assertEquals(30, policy.getBackoff(1).toSeconds());
    assertEquals(180, policy.getBackoff(2).toSeconds());
    assertEquals(1080, policy.getBackoff(3).toSeconds());
    assertEquals(30, policy.getBackoff(4).toMinutes());
    assertEquals(30, policy.getBackoff(5).toMinutes());
  }

}
