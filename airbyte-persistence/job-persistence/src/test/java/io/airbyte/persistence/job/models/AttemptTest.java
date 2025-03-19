/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.models;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.config.Attempt;
import io.airbyte.config.AttemptStatus;
import org.junit.jupiter.api.Test;

class AttemptTest {

  @Test
  void testIsAttemptInTerminalState() {
    assertFalse(Attempt.isAttemptInTerminalState(attemptWithStatus(AttemptStatus.RUNNING)));
    assertTrue(Attempt.isAttemptInTerminalState(attemptWithStatus(AttemptStatus.FAILED)));
    assertTrue(Attempt.isAttemptInTerminalState(attemptWithStatus(AttemptStatus.SUCCEEDED)));
  }

  private static Attempt attemptWithStatus(final AttemptStatus attemptStatus) {
    return new Attempt(1, 1L, null, null, null, attemptStatus, null, null, 0L, 0L, null);
  }

}
