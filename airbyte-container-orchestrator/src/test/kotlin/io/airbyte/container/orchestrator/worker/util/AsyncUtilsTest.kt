/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.util

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.slf4j.MDC
import java.lang.Thread.sleep
import kotlin.coroutines.suspendCoroutine

private const val MDC_KEY = "test"
private const val MDC_VALUE = "value"

class AsyncUtilsTest {
  @Test
  fun `runAsync should preserve the MDC context even if suspended`() {
    val mdcMap = mutableMapOf(MDC_KEY to MDC_VALUE)
    runBlocking {
      AsyncUtils.runAsync(Dispatchers.Unconfined, this, mdcMap) {
        delay(100)
        assertEquals(MDC_VALUE, MDC.get(MDC_KEY))
      }
    }
  }

  @Test
  fun `runLaunch should preserve the MDC context even if suspended`() {
    val mdcMap = mutableMapOf(MDC_KEY to MDC_VALUE)
    runBlocking {
      AsyncUtils.runLaunch(Dispatchers.Unconfined, this, mdcMap) {
        delay(100)
        assertEquals(MDC_VALUE, MDC.get(MDC_KEY))
      }
    }
  }

  @Test
  fun `runLaunch switching between threads should preserve the MDC context even if suspended`() {
    val mdcMap = mutableMapOf(MDC_KEY to MDC_VALUE)
    runBlocking {
      AsyncUtils.runLaunch(Dispatchers.Unconfined, this, mdcMap) {
        withContext(Dispatchers.IO) {
          delay(100)
          assertEquals(MDC_VALUE, MDC.get(MDC_KEY))
        }
        withContext(Dispatchers.Default) {
          delay(100)
          assertEquals(MDC_VALUE, MDC.get(MDC_KEY))
        }
        suspendCoroutine { continuation ->
          sleep(100)
          assertEquals(MDC_VALUE, MDC.get(MDC_KEY))
          continuation.resumeWith(Result.success(Unit))
        }
        delay(100)
        assertEquals(MDC_VALUE, MDC.get(MDC_KEY))
      }
    }
  }
}
