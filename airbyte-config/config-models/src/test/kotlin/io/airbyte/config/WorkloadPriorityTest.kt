/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource

class WorkloadPriorityTest {
  @ParameterizedTest
  @MethodSource("priorityMatrix")
  fun `toInt maps a priority into the expected int`(
    expectedInt: Int,
    priority: WorkloadPriority,
  ) {
    val result = priority.toInt()
    assertEquals(expectedInt, result)
  }

  @ParameterizedTest
  @MethodSource("priorityMatrix")
  fun `fromInt maps an int into the expected priority`(
    int: Int,
    expectedPriority: WorkloadPriority,
  ) {
    val result = WorkloadPriority.fromInt(int)
    assertEquals(expectedPriority, result)
  }

  companion object {
    @JvmStatic
    fun priorityMatrix() =
      listOf(
        Arguments.of(0, WorkloadPriority.DEFAULT),
        Arguments.of(1, WorkloadPriority.HIGH),
      )
  }
}
