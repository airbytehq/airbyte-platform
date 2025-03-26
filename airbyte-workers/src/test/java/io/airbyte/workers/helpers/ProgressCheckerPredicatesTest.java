/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.airbyte.api.client.model.generated.AttemptStats;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ProgressCheckerPredicatesTest {

  final ProgressCheckerPredicates checker = new ProgressCheckerPredicates();

  @ParameterizedTest
  @MethodSource("statsProgressMatrix")
  void checkAttemptStats(final AttemptStats stats, final boolean expected) {
    assertEquals(expected, checker.test(stats));
  }

  public static Stream<Arguments> statsProgressMatrix() {
    return Stream.of(
        Arguments.of(new AttemptStats(null, null, null, null, 0L, null, null), false),
        Arguments.of(new AttemptStats(null, null, null, null, 1L, null, null), true),
        Arguments.of(new AttemptStats(null, null, null, null, 3L, null, null), true),
        Arguments.of(new AttemptStats(null, null, null, null, 9999L, null, null), true),
        Arguments.of(new AttemptStats(null, null, null, null, null, null, null), false));
  }

}
