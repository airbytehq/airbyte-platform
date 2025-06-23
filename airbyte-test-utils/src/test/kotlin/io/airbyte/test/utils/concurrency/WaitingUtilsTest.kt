/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils.concurrency

import io.airbyte.test.utils.concurrency.WaitingUtils.waitForCondition
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.time.Duration
import java.util.function.Supplier

internal class WaitingUtilsTest {
  @Test
  fun testWaitForConditionConditionMet() {
    val condition: Supplier<Boolean> = Mockito.mock(Supplier::class.java) as Supplier<Boolean>
    Mockito
      .`when`(condition.get())
      .thenReturn(false)
      .thenReturn(false)
      .thenReturn(true)
    Assertions.assertTrue(waitForCondition(Duration.ofMillis(1), Duration.ofMillis(5), condition))
  }

  @Test
  fun testWaitForConditionTimeout() {
    val condition: Supplier<Boolean> = Mockito.mock(Supplier::class.java) as Supplier<Boolean>
    Mockito.`when`(condition.get()).thenReturn(false)
    Assertions.assertFalse(waitForCondition(Duration.ofMillis(1), Duration.ofMillis(5), condition))
  }
}
