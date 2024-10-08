/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging.logback

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.pattern.ThrowableProxyConverter
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.ThrowableProxy
import ch.qos.logback.core.CoreConstants.LINE_SEPARATOR
import io.airbyte.commons.constants.AirbyteSecretConstants.SECRETS_MASK
import io.airbyte.commons.logging.LoggingHelper
import io.airbyte.commons.logging.LoggingHelper.LOG_SOURCE_MDC_KEY
import io.airbyte.commons.logging.LoggingHelper.SOURCE_LOGGER_PREFIX
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

private class AirbyteOperationsJobLogbackMessageLayoutTest {
  @Test
  fun testLogMessage() {
    val logSource = LoggingHelper.applyColor(LoggingHelper.Color.BLUE_BACKGROUND, SOURCE_LOGGER_PREFIX)
    val context = mapOf(LOG_SOURCE_MDC_KEY to logSource)
    val className = "io.airbyte.TestClass"
    val methodName = "testMethod"
    val fileName = "TestClass.kt"
    val lineNumber = 12345
    val logLevel = Level.INFO
    val logMessage = "test message"
    val logThreadName = "Test Thread"
    val timestamp = 0L
    val event =
      mockk<ILoggingEvent> {
        every { callerData } returns arrayOf(StackTraceElement(className, methodName, fileName, lineNumber))
        every { formattedMessage } returns logMessage
        every { level } returns logLevel
        every { loggerName } returns OPERATIONS_JOB_LOGGER_NAME
        every { mdcPropertyMap } returns context
        every { threadName } returns logThreadName
        every { throwableProxy } returns null
        every { timeStamp } returns timestamp
      }

    val layout = AirbyteOperationsJobLogbackMessageLayout()
    val message = layout.doLayout(loggingEvent = event)

    val expected =
      buildString {
        append("1970-01-01 00:00:00 ")
        append(logSource)
        append(" > $logMessage$LINE_SEPARATOR")
      }
    assertEquals(expected, message)
  }

  @Test
  fun testLogMessageWithMaskedData() {
    val logSource = LoggingHelper.applyColor(LoggingHelper.Color.BLUE_BACKGROUND, SOURCE_LOGGER_PREFIX)
    val context = mapOf(LOG_SOURCE_MDC_KEY to logSource)
    val className = "io.airbyte.TestClass"
    val methodName = "testMethod"
    val fileName = "TestClass.kt"
    val lineNumber = 12345
    val logLevel = Level.INFO
    val apiKey = UUID.randomUUID().toString()
    val logMessage = "test message (\"api_token\":\"$apiKey\")"
    val logThreadName = "Test Thread"
    val timestamp = 0L
    val event =
      mockk<ILoggingEvent> {
        every { callerData } returns arrayOf(StackTraceElement(className, methodName, fileName, lineNumber))
        every { formattedMessage } returns logMessage
        every { level } returns logLevel
        every { loggerName } returns OPERATIONS_JOB_LOGGER_NAME
        every { mdcPropertyMap } returns context
        every { threadName } returns logThreadName
        every { throwableProxy } returns null
        every { timeStamp } returns timestamp
      }

    val layout = AirbyteOperationsJobLogbackMessageLayout()
    val message = layout.doLayout(loggingEvent = event)

    val expected =
      buildString {
        append("1970-01-01 00:00:00 ")
        append(logSource)
        append(" > ${logMessage.replace(apiKey, SECRETS_MASK)}$LINE_SEPARATOR")
      }
    assertEquals(expected, message)
  }

  @Test
  fun testLogMessageWithException() {
    val throwableConverter = ThrowableProxyConverter()
    throwableConverter.start()

    val logSource = LoggingHelper.applyColor(LoggingHelper.Color.BLUE_BACKGROUND, SOURCE_LOGGER_PREFIX)
    val context = mapOf(LOG_SOURCE_MDC_KEY to logSource)
    val className = "io.airbyte.TestClass"
    val methodName = "testMethod"
    val fileName = "TestClass.kt"
    val lineNumber = 12345
    val logLevel = Level.INFO
    val logMessage = "test message"
    val logThreadName = "Test Thread"
    val timestamp = 0L
    val exception = RuntimeException("test", NullPointerException("root"))
    val event =
      mockk<ILoggingEvent> {
        every { callerData } returns arrayOf(StackTraceElement(className, methodName, fileName, lineNumber))
        every { formattedMessage } returns logMessage
        every { level } returns logLevel
        every { loggerName } returns OPERATIONS_JOB_LOGGER_NAME
        every { mdcPropertyMap } returns context
        every { threadName } returns logThreadName
        every { throwableProxy } returns ThrowableProxy(exception)
        every { timeStamp } returns timestamp
      }

    val layout = AirbyteOperationsJobLogbackMessageLayout()
    val message = layout.doLayout(loggingEvent = event)

    val expected =
      buildString {
        append("1970-01-01 00:00:00 ")
        append(logSource)
        append(" > $logMessage")
        append("$LINE_SEPARATOR${throwableConverter.convert(event)}")
        append(LINE_SEPARATOR)
      }
    assertEquals(expected, message)
  }
}
