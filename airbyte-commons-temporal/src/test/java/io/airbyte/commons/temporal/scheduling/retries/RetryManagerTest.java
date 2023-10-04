/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling.retries;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class RetryManagerTest {

  @ParameterizedTest
  @MethodSource("limitMatrix")
  void checksConfiguredLimits(final int successiveCompleteFailureLimit,
                              final int successivePartialFailureLimit,
                              final int totalCompleteFailureLimit,
                              final int totalPartialFailureLimit,
                              final boolean shouldRetry1,
                              final boolean shouldRetry2,
                              final boolean shouldRetry3,
                              final boolean shouldRetry4,
                              final boolean shouldRetry5,
                              final boolean shouldRetry6) {
    // configure limits
    final var manager = RetryManager.builder()
        .successiveCompleteFailureLimit(successiveCompleteFailureLimit)
        .successivePartialFailureLimit(successivePartialFailureLimit)
        .totalCompleteFailureLimit(totalCompleteFailureLimit)
        .totalPartialFailureLimit(totalPartialFailureLimit)
        .build();

    // simulate some failures and check `shouldRetry` returns value expected in each state
    manager.incrementFailure();
    assertEquals(shouldRetry1, manager.shouldRetry());
    manager.incrementFailure();
    manager.incrementFailure();
    assertEquals(shouldRetry2, manager.shouldRetry());
    manager.incrementPartialFailure();
    manager.incrementPartialFailure();
    manager.incrementFailure();
    assertEquals(shouldRetry3, manager.shouldRetry());
    manager.incrementPartialFailure();
    manager.incrementPartialFailure();
    manager.incrementPartialFailure();
    assertEquals(shouldRetry4, manager.shouldRetry());
    manager.incrementFailure();
    assertEquals(shouldRetry5, manager.shouldRetry());
    manager.incrementPartialFailure();
    assertEquals(shouldRetry6, manager.shouldRetry());
  }

  private static Stream<Arguments> limitMatrix() {
    return Stream.of(
        Arguments.of(1, 10, 10, 10, false, false, false, true, false, true),
        Arguments.of(3, 10, 10, 10, true, false, true, true, true, true),
        Arguments.of(10, 2, 10, 10, true, true, true, false, true, true),
        Arguments.of(10, 10, 10, 6, true, true, true, true, true, false),
        Arguments.of(10, 10, 5, 10, true, true, true, true, false, false),
        Arguments.of(10, 5, 10, 10, true, true, true, true, true, true),
        Arguments.of(10, 10, 1, 10, false, false, false, false, false, false),
        Arguments.of(10, 10, 10, 5, true, true, true, false, false, false));
  }

  @Test
  void noBackoffIfNoMatchingBackoffPolicy() {
    final var manager = RetryManager.builder().build();

    manager.incrementFailure(true);
    assertEquals(manager.getBackoff(), Duration.ZERO);
    manager.incrementFailure(false);
    assertEquals(manager.getBackoff(), Duration.ZERO);
  }

  @ParameterizedTest
  @MethodSource("backoffPolicyMatrix")
  void delegatesToBackoffPolicy(final BackoffPolicy fb,
                                final BackoffPolicy sfb,
                                final boolean failureIsPartial,
                                final int expectedFBCalls,
                                final int expectedSFBCalls) {
    final var manager = RetryManager.builder()
        .completeFailureBackoffPolicy(fb)
        .partialFailureBackoffPolicy(sfb)
        .build();

    manager.incrementFailure(failureIsPartial);
    manager.getBackoff();

    verify(fb, times(expectedFBCalls)).getBackoff(anyLong());
    verify(sfb, times(expectedSFBCalls)).getBackoff(anyLong());
  }

  private static Stream<Arguments> backoffPolicyMatrix() {
    return Stream.of(
        Arguments.of(mock(BackoffPolicy.class), mock(BackoffPolicy.class), true, 0, 1),
        Arguments.of(mock(BackoffPolicy.class), mock(BackoffPolicy.class), false, 1, 0));
  }

}
