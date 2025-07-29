/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import io.airbyte.config.FailureReason
import io.airbyte.workers.helper.MAX_FAILURES_TO_KEEP
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class ReplicationWorkerStateTest {
  private lateinit var replicationWorkerState: ReplicationWorkerState

  @BeforeEach
  fun setUp() {
    replicationWorkerState = ReplicationWorkerState()
  }

  @Test
  fun `trackFailure prevents accumulation beyond limit`() {
    val jobId = 12345L
    val attempt = 1

    // Add failures up to the limit
    repeat(MAX_FAILURES_TO_KEEP) { i ->
      replicationWorkerState.trackFailure(RuntimeException("Error ${i + 1}"), jobId, attempt)
    }

    // Should have exactly the limit number of failures
    assertEquals(MAX_FAILURES_TO_KEEP, replicationWorkerState.getFailures().size)

    // Add one more failure - should trigger truncation notice
    replicationWorkerState.trackFailure(RuntimeException("Error ${MAX_FAILURES_TO_KEEP + 1}"), jobId, attempt)

    // Should have limit + 1 (truncation notice)
    assertEquals(MAX_FAILURES_TO_KEEP + 1, replicationWorkerState.getFailures().size)

    // Last failure should be truncation notice
    val truncationNotice = replicationWorkerState.getFailures().last()
    assertEquals(FailureReason.FailureType.SYSTEM_ERROR, truncationNotice.failureType)
    assertEquals(FailureReason.FailureOrigin.AIRBYTE_PLATFORM, truncationNotice.failureOrigin)
    assertEquals("Truncated additional failures to prevent serialization issues", truncationNotice.internalMessage)

    // Add more failures - should be ignored
    repeat(5) { i ->
      replicationWorkerState.trackFailure(RuntimeException("Ignored Error ${i + 1}"), jobId, attempt)
    }

    // Should still have limit + 1 (no new failures added)
    assertEquals(MAX_FAILURES_TO_KEEP + 1, replicationWorkerState.getFailures().size)
  }

  @Test
  fun `trackFailure allows accumulation under limit`() {
    val jobId = 12345L
    val attempt = 1

    // Add failures under the limit
    repeat(MAX_FAILURES_TO_KEEP - 2) { i ->
      replicationWorkerState.trackFailure(RuntimeException("Error ${i + 1}"), jobId, attempt)
    }

    // Should have exactly the number we added
    assertEquals(MAX_FAILURES_TO_KEEP - 2, replicationWorkerState.getFailures().size)

    // All failures should be regular failures (no truncation notice)
    replicationWorkerState.getFailures().forEach { failure ->
      assertTrue(failure.internalMessage?.startsWith("Error") == true)
    }
  }

  @Test
  fun `trackFailure handles empty state correctly`() {
    // Initially should be empty
    assertEquals(0, replicationWorkerState.getFailures().size)

    // Add one failure
    replicationWorkerState.trackFailure(RuntimeException("First Error"), 12345L, 1)

    // Should have exactly one failure
    assertEquals(1, replicationWorkerState.getFailures().size)
    assertTrue(replicationWorkerState.getFailures()[0].internalMessage?.contains("First Error") == true)
  }
}
