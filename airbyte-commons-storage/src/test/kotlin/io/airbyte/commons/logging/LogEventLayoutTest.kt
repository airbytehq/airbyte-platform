/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.pattern.ThrowableProxyConverter
import ch.qos.logback.core.CoreConstants.LINE_SEPARATOR
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class LogEventLayoutTest {
  private lateinit var logUtils: LogUtils
  private lateinit var layout: LogEventLayout

  @BeforeEach
  fun setup() {
    logUtils = LogUtils()
    logUtils.init()
    layout = LogEventLayout(logUtils = logUtils)
  }

  @AfterEach
  fun teardown() {
    logUtils.close()
  }

  @Test
  fun testLayout() {
    val logSource = LogSource.SOURCE
    val className = "io.airbyte.TestClass"
    val methodName = "testMethod"
    val lineNumber = 12345
    val logLevel = Level.INFO
    val logMessage = "test message"
    val logThreadName = "Test Thread"
    val timestamp = 0L
    val logCaller =
      LogCaller(
        className = className,
        methodName = methodName,
        lineNumber = lineNumber,
        threadName = logThreadName,
      )
    val logEvent =
      LogEvent(
        timestamp = timestamp,
        message = logMessage,
        level = logLevel.toString(),
        logSource = logSource,
        caller = logCaller,
        throwable = null,
      )

    val result = layout.doLayout(logEvent = logEvent)

    val expected =
      buildString {
        append("1970-01-01 00:00:00 ")
        append(layout.formatLogSource(logSource))
        append(" > $logMessage$LINE_SEPARATOR")
      }
    assertEquals(expected, result)
  }

  @Test
  fun testLayoutWithThrowable() {
    val throwableConverter = ThrowableProxyConverter()
    throwableConverter.start()

    val logSource = LogSource.SOURCE
    val className = "io.airbyte.TestClass"
    val methodName = "testMethod"
    val lineNumber = 12345
    val logLevel = Level.INFO
    val logMessage = "test message"
    val logThreadName = "Test Thread"
    val timestamp = 0L
    val exception = RuntimeException("test", NullPointerException("root"))

    val logCaller =
      LogCaller(
        className = className,
        methodName = methodName,
        lineNumber = lineNumber,
        threadName = logThreadName,
      )
    val logEvent =
      LogEvent(
        timestamp = timestamp,
        message = logMessage,
        level = logLevel.toString(),
        logSource = logSource,
        caller = logCaller,
        throwable = exception,
      )

    val result = layout.doLayout(logEvent = logEvent)

    val expected =
      buildString {
        append("1970-01-01 00:00:00 ")
        append(layout.formatLogSource(logSource))
        append(" > $logMessage")
        append("$LINE_SEPARATOR${layout.generateStackTrace(exception)}")
        append(LINE_SEPARATOR)
      }
    assertEquals(expected, result)
  }
}
