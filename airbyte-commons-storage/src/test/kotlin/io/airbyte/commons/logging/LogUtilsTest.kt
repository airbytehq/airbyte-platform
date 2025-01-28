/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import java.io.PrintWriter
import java.io.StringWriter

internal class LogUtilsTest {
  private lateinit var logUtils: LogUtils

  @BeforeEach
  fun setup() {
    logUtils = LogUtils()
    logUtils.init()
  }

  @AfterEach
  fun teardown() {
    logUtils.close()
  }

  @Test
  fun testConvertingThrowableToStackTrace() {
    val message = "test message"
    val throwable = NullPointerException(message)
    val output = StringWriter()
    throwable.printStackTrace(PrintWriter(output))
    val converted = logUtils.convertThrowableToStackTrace(throwable = throwable)
    assertNotNull(converted)
    assertEquals(output.toString(), converted)
  }

  @Test
  fun testConvertingNullThrowableToStackTrace() {
    assertDoesNotThrow {
      val converted = logUtils.convertThrowableToStackTrace(throwable = null)
      assertNull(converted)
    }
  }
}
