/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.lang

import io.airbyte.commons.lang.Exceptions.swallow
import io.airbyte.commons.lang.Exceptions.toRuntime
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.io.IOException

internal class ExceptionsTest {
  @Test
  fun testToRuntime() {
    Assertions.assertEquals("hello", toRuntime { callable("hello", false) })
    Assertions.assertThrows(
      RuntimeException::class.java,
    ) { toRuntime<String?> { callable("goodbye", true) } }
  }

  @Test
  fun testToRuntimeVoid() {
    val list: MutableList<String?> = mutableListOf()
    Assertions.assertThrows(
      RuntimeException::class.java,
    ) { toRuntime { voidCallable(list, "hello", true) } }
    Assertions.assertEquals(0, list.size)

    toRuntime { voidCallable(list, "goodbye", false) }
    Assertions.assertEquals(1, list.size)
    Assertions.assertEquals("goodbye", list[0])
  }

  @Test
  fun testSwallow() {
    swallow {
      throw RuntimeException()
    }
  }

  private fun callable(
    input: String?,
    shouldThrow: Boolean,
  ): String? {
    if (shouldThrow) {
      throw IOException()
    } else {
      return input
    }
  }

  private fun voidCallable(
    list: MutableList<String?>,
    input: String?,
    shouldThrow: Boolean,
  ) {
    if (shouldThrow) {
      throw IOException()
    } else {
      list.add(input)
    }
  }
}
