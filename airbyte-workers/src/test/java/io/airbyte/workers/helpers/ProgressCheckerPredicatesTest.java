/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
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
        Arguments.of(new AttemptStats().recordsCommitted(0L), false),
        Arguments.of(new AttemptStats().recordsCommitted(1L), true),
        Arguments.of(new AttemptStats().recordsCommitted(3L), true),
        Arguments.of(new AttemptStats().recordsCommitted(9999L), true),
        Arguments.of(new AttemptStats().recordsCommitted(null), false));
  }

}
