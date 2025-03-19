/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging.logback

import ch.qos.logback.classic.spi.ILoggingEvent
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

internal class AirbyteMdcEvaluatorTest {
  @Test
  fun testEvaluateMdc() {
    val contextKey = "key"
    val evaluator = AirbyteMdcEvaluator(contextKey)
    val mdc = mutableMapOf<String, String?>(contextKey to "non-blank")
    val event =
      mockk<ILoggingEvent> {
        every { mdcPropertyMap } returns mdc
      }

    assertFalse(evaluator.evaluate(event))

    mdc[contextKey] = ""
    assertTrue(evaluator.evaluate(event))

    mdc[contextKey] = null
    assertTrue(evaluator.evaluate(event))
  }
}
