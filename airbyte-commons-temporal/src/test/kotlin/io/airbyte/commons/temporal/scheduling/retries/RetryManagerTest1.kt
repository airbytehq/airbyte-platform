/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling.retries

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import java.time.Duration
import java.util.stream.Stream

internal class RetryManagerTest1 {
  @ParameterizedTest
  @MethodSource("limitMatrix")
  fun checksConfiguredLimits(
    successiveCompleteFailureLimit: Int,
    successivePartialFailureLimit: Int,
    totalCompleteFailureLimit: Int,
    totalPartialFailureLimit: Int,
    shouldRetry1: Boolean,
    shouldRetry2: Boolean,
    shouldRetry3: Boolean,
    shouldRetry4: Boolean,
    shouldRetry5: Boolean,
    shouldRetry6: Boolean,
  ) {
    // configure limits
    val manager =
      RetryManager(
        null,
        null,
        successiveCompleteFailureLimit,
        successivePartialFailureLimit,
        totalCompleteFailureLimit,
        totalPartialFailureLimit,
      )

    // simulate some failures and check `shouldRetry` returns value expected in each state
    manager.incrementFailure()
    Assertions.assertEquals(shouldRetry1, manager.shouldRetry())
    manager.incrementFailure()
    manager.incrementFailure()
    Assertions.assertEquals(shouldRetry2, manager.shouldRetry())
    manager.incrementPartialFailure()
    manager.incrementPartialFailure()
    manager.incrementFailure()
    Assertions.assertEquals(shouldRetry3, manager.shouldRetry())
    manager.incrementPartialFailure()
    manager.incrementPartialFailure()
    manager.incrementPartialFailure()
    Assertions.assertEquals(shouldRetry4, manager.shouldRetry())
    manager.incrementFailure()
    Assertions.assertEquals(shouldRetry5, manager.shouldRetry())
    manager.incrementPartialFailure()
    Assertions.assertEquals(shouldRetry6, manager.shouldRetry())
  }

  @Test
  fun noBackoffIfNoMatchingBackoffPolicy() {
    val manager = RetryManager(null, null, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE)

    manager.incrementFailure(true)
    Assertions.assertEquals(Duration.ZERO, manager.backoff)
    manager.incrementFailure(false)
    Assertions.assertEquals(Duration.ZERO, manager.backoff)
  }

  @ParameterizedTest
  @MethodSource("backoffPolicyMatrix")
  fun delegatesToBackoffPolicy(
    fb: BackoffPolicy,
    sfb: BackoffPolicy,
    failureIsPartial: Boolean,
    expectedFBCalls: Int,
    expectedSFBCalls: Int,
  ) {
    val manager = RetryManager(fb, sfb, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE)

    manager.incrementFailure(failureIsPartial)
    manager.backoff

    Mockito.verify(fb, Mockito.times(expectedFBCalls)).getBackoff(ArgumentMatchers.anyLong())
    Mockito.verify(sfb, Mockito.times(expectedSFBCalls)).getBackoff(ArgumentMatchers.anyLong())
  }

  companion object {
    @JvmStatic
    private fun limitMatrix(): Stream<Arguments?> =
      Stream.of<Arguments?>(
        Arguments.of(1, 10, 10, 10, false, false, false, true, false, true),
        Arguments.of(3, 10, 10, 10, true, false, true, true, true, true),
        Arguments.of(10, 2, 10, 10, true, true, true, false, true, true),
        Arguments.of(10, 10, 10, 6, true, true, true, true, true, false),
        Arguments.of(10, 10, 5, 10, true, true, true, true, false, false),
        Arguments.of(10, 5, 10, 10, true, true, true, true, true, true),
        Arguments.of(10, 10, 1, 10, false, false, false, false, false, false),
        Arguments.of(10, 10, 10, 5, true, true, true, false, false, false),
      )

    @JvmStatic
    private fun backoffPolicyMatrix(): Stream<Arguments?> =
      Stream.of<Arguments?>(
        Arguments.of(
          Mockito.mock(BackoffPolicy::class.java),
          Mockito.mock(BackoffPolicy::class.java),
          true,
          0,
          1,
        ),
        Arguments.of(
          Mockito.mock(BackoffPolicy::class.java),
          Mockito.mock(BackoffPolicy::class.java),
          false,
          1,
          0,
        ),
      )
  }
}
