/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.text

import io.airbyte.commons.text.Names.singleQuote
import io.airbyte.commons.text.Names.toAlphanumericAndUnderscore
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable

internal class NamesTest {
  @Test
  fun testToAlphanumericAndUnderscore() {
    Assertions.assertEquals("users", toAlphanumericAndUnderscore("users"))
    Assertions.assertEquals("users123", toAlphanumericAndUnderscore("users123"))
    Assertions.assertEquals("UsErS", toAlphanumericAndUnderscore("UsErS"))
    Assertions.assertEquals("users_USE_special_____", toAlphanumericAndUnderscore("users USE special !@#$"))
  }

  @Test
  fun testSimpleQuote() {
    Assertions.assertEquals("'abc'", singleQuote("abc"))
    Assertions.assertEquals("'abc'", singleQuote("'abc'"))
    Assertions.assertThrows<IllegalStateException?>(IllegalStateException::class.java, Executable { singleQuote("'abc") })
    Assertions.assertThrows<IllegalStateException?>(IllegalStateException::class.java, Executable { singleQuote("abc'") })
  }
}
