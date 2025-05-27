/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.util

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.slf4j.MDC

class AsyncUtilsTest {
  @Test
  fun `runAsync should preserve the MDC context even if suspended`() {
    runBlocking {
      val mdcMap = mutableMapOf("test" to "value")
      AsyncUtils.runAsync(Dispatchers.Unconfined, this, mdcMap) {
        delay(100)
        assertEquals("value", MDC.get("test"))
      }
    }
  }

  @Test
  fun `runLaunch should preserve the MDC context even if suspended`() {
    runBlocking {
      val mdcMap = mutableMapOf("test" to "value")
      AsyncUtils.runLaunch(Dispatchers.Unconfined, this, mdcMap) {
        delay(100)
        assertEquals("value", MDC.get("test"))
      }
    }
  }
}
