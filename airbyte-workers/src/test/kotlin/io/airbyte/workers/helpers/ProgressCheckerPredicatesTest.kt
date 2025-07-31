/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers

import io.airbyte.api.client.model.generated.AttemptStats
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

internal class ProgressCheckerPredicatesTest {
  val checker: ProgressCheckerPredicates = ProgressCheckerPredicates()

  @ParameterizedTest
  @MethodSource("statsProgressMatrix")
  fun checkAttemptStats(
    stats: AttemptStats,
    expected: Boolean,
  ) {
    Assertions.assertEquals(expected, checker.test(stats))
  }

  companion object {
    @JvmStatic
    fun statsProgressMatrix(): Stream<Arguments> =
      Stream.of(
        Arguments.of(AttemptStats(recordsCommitted = 0L), false),
        Arguments.of(AttemptStats(recordsCommitted = 1L), true),
        Arguments.of(AttemptStats(recordsCommitted = 3L), true),
        Arguments.of(AttemptStats(recordsCommitted = 3L, recordsRejected = 2L), true),
        Arguments.of(AttemptStats(recordsRejected = 5L), true),
        Arguments.of(AttemptStats(recordsCommitted = 9999L), true),
        Arguments.of(AttemptStats(recordsCommitted = null), false),
      )
  }
}
