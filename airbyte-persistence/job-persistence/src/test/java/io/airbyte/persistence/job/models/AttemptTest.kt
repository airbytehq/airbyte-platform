/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.models

import io.airbyte.config.Attempt
import io.airbyte.config.Attempt.Companion.isAttemptInTerminalState
import io.airbyte.config.AttemptStatus
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class AttemptTest {
  @Test
  fun testIsAttemptInTerminalState() {
    Assertions.assertFalse(isAttemptInTerminalState(attemptWithStatus(AttemptStatus.RUNNING)))
    Assertions.assertTrue(isAttemptInTerminalState(attemptWithStatus(AttemptStatus.FAILED)))
    Assertions.assertTrue(isAttemptInTerminalState(attemptWithStatus(AttemptStatus.SUCCEEDED)))
  }

  companion object {
    private fun attemptWithStatus(attemptStatus: AttemptStatus): Attempt = Attempt(1, 1L, null, null, null, attemptStatus, null, null, 0L, 0L, null)
  }
}
