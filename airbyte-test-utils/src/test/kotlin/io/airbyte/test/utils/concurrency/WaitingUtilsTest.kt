/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils.concurrency

import io.airbyte.test.utils.concurrency.WaitingUtils.waitForCondition
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.function.Supplier

internal class WaitingUtilsTest {
  @Test
  fun testWaitForConditionConditionMet() {
    val condition: Supplier<Boolean> = mockk()
    every { condition.get() } returnsMany listOf(false, false, true)
    Assertions.assertTrue(waitForCondition(Duration.ofMillis(1), Duration.ofMillis(5), condition))
  }

  @Test
  fun testWaitForConditionTimeout() {
    val condition: Supplier<Boolean> = mockk()
    every { condition.get() } returns false
    Assertions.assertFalse(waitForCondition(Duration.ofMillis(1), Duration.ofMillis(5), condition))
  }
}
