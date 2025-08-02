/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.lang

import io.airbyte.commons.lang.Exceptions.swallow
import io.airbyte.commons.lang.Exceptions.toRuntime
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.io.IOException
import java.util.concurrent.Callable

internal class ExceptionsTest {
  @Test
  fun testToRuntime() {
    Assertions.assertEquals("hello", toRuntime<String?>(Callable { callable("hello", false) }))
    Assertions.assertThrows<RuntimeException?>(
      RuntimeException::class.java,
      Executable { toRuntime<String?>(Callable { callable("goodbye", true) }) },
    )
  }

  @Test
  fun testToRuntimeVoid() {
    val list: MutableList<String?> = ArrayList<String?>()
    Assertions.assertThrows<RuntimeException?>(
      RuntimeException::class.java,
      Executable { Exceptions.toRuntime { voidCallable(list, "hello", true) } },
    )
    Assertions.assertEquals(0, list.size)

    Exceptions.toRuntime { voidCallable(list, "goodbye", false) }
    Assertions.assertEquals(1, list.size)
    Assertions.assertEquals("goodbye", list.get(0))
  }

  @Test
  fun testSwallow() {
    swallow {
      throw RuntimeException()
    }
  }

  @Throws(IOException::class)
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

  @Throws(IOException::class)
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
