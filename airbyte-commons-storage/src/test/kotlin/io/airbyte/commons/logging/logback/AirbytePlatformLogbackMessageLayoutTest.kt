/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging.logback

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.pattern.ThrowableProxyConverter
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.ThrowableProxy
import ch.qos.logback.core.CoreConstants.DASH_CHAR
import ch.qos.logback.core.CoreConstants.LINE_SEPARATOR
import ch.qos.logback.core.CoreConstants.TAB
import ch.qos.logback.core.pattern.color.ANSIConstants
import ch.qos.logback.core.pattern.color.ANSIConstants.ESC_END
import ch.qos.logback.core.pattern.color.ANSIConstants.ESC_START
import io.airbyte.commons.constants.AirbyteSecretConstants.SECRETS_MASK
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

private class AirbytePlatformLogbackMessageLayoutTest {
  @BeforeEach
  fun setup() {
    System.setProperty(CI_MODE_SYSTEM_PROPERTY, "false")
  }

  @Test
  fun testCiModeLogMessage() {
    System.setProperty(CI_MODE_SYSTEM_PROPERTY, "true")
    val spanId = UUID.randomUUID().toString()
    val traceId = UUID.randomUUID().toString()
    val context =
      mapOf(
        DATADOG_SPAN_ID_KEY to spanId,
        DATADOG_TRACE_ID_KEY to traceId,
      )
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
        every { loggerName } returns PLATFORM_LOGGER_NAME
        every { mdcPropertyMap } returns context
        every { threadName } returns logThreadName
        every { throwableProxy } returns null
        every { timeStamp } returns timestamp
      }

    val layout = AirbytePlatformLogbackMessageLayout()
    val message = layout.doLayout(loggingEvent = event)

    val expected =
      buildString {
        append("1970-01-01 00:00:00,000 [dd.trace_id=$traceId dd.span_id=$spanId] ")
        append("[$logThreadName]$TAB$ESC_START${ANSIConstants.BLUE_FG}$ESC_END${logLevel}$DEFAULT_COLOR$TAB")
        append("${formatClassName(className)}($methodName):$lineNumber $DASH_CHAR ")
        append("$logMessage$LINE_SEPARATOR")
      }
    assertEquals(expected, message)
  }

  @Test
  fun tesLogMessage() {
    val context = emptyMap<String, String>()
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
        every { loggerName } returns PLATFORM_LOGGER_NAME
        every { mdcPropertyMap } returns context
        every { threadName } returns logThreadName
        every { throwableProxy } returns null
        every { timeStamp } returns timestamp
      }

    val layout = AirbytePlatformLogbackMessageLayout()
    val message = layout.doLayout(loggingEvent = event)

    val expected =
      buildString {
        append("1970-01-01 00:00:00,000 ")
        append("[$logThreadName]$TAB$ESC_START${ANSIConstants.BLUE_FG}$ESC_END${logLevel}$DEFAULT_COLOR$TAB")
        append("${formatClassName(className)}($methodName):$lineNumber $DASH_CHAR ")
        append("$logMessage$LINE_SEPARATOR")
      }
    assertEquals(expected, message)
  }

  @Test
  fun tesLogMessageWithMaskedData() {
    val context = emptyMap<String, String>()
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
        every { loggerName } returns PLATFORM_LOGGER_NAME
        every { mdcPropertyMap } returns context
        every { threadName } returns logThreadName
        every { throwableProxy } returns null
        every { timeStamp } returns timestamp
      }

    val layout = AirbytePlatformLogbackMessageLayout()
    val message = layout.doLayout(loggingEvent = event)

    val expected =
      buildString {
        append("1970-01-01 00:00:00,000 ")
        append("[$logThreadName]$TAB$ESC_START${ANSIConstants.BLUE_FG}$ESC_END${logLevel}$DEFAULT_COLOR$TAB")
        append("${formatClassName(className)}($methodName):$lineNumber $DASH_CHAR ")
        append("${logMessage.replace(apiKey, SECRETS_MASK)}$LINE_SEPARATOR")
      }
    assertEquals(expected, message)
  }

  @Test
  fun tesLogMessageWithException() {
    val throwableConverter = ThrowableProxyConverter()
    throwableConverter.start()

    val context = emptyMap<String, String>()
    val className = "io.airbyte.TestClass"
    val methodName = "testMethod"
    val fileName = "TestClass.kt"
    val lineNumber = 12345
    val logLevel = Level.ERROR
    val logMessage = "test message"
    val logThreadName = "Test Thread"
    val exception = RuntimeException("test", NullPointerException("root"))
    val timestamp = 0L
    val event =
      mockk<ILoggingEvent> {
        every { callerData } returns arrayOf(StackTraceElement(className, methodName, fileName, lineNumber))
        every { formattedMessage } returns logMessage
        every { level } returns logLevel
        every { loggerName } returns PLATFORM_LOGGER_NAME
        every { mdcPropertyMap } returns context
        every { threadName } returns logThreadName
        every { throwableProxy } returns ThrowableProxy(exception)
        every { timeStamp } returns timestamp
      }

    val layout = AirbytePlatformLogbackMessageLayout()
    val message = layout.doLayout(loggingEvent = event)

    val expected =
      buildString {
        append("1970-01-01 00:00:00,000 ")
        append("[$logThreadName]$TAB$ESC_START${ANSIConstants.BOLD + ANSIConstants.RED_FG}$ESC_END${logLevel}$DEFAULT_COLOR$TAB")
        append("${formatClassName(className)}($methodName):$lineNumber $DASH_CHAR ")
        append(logMessage)
        append("$LINE_SEPARATOR${throwableConverter.convert(event)}")
        append(LINE_SEPARATOR)
      }
    assertEquals(expected, message)
  }

  @Test
  fun tesLogMessageWithCallerContext() {
    val callerClassName = "io.airbyte.CallerTestClass"
    val callerMethodName = "callerTestMethod"
    val callerLineNumber = "999"
    val callerThreadName = "Caller Test Thread"
    val context =
      mapOf(
        CALLER_QUALIFIED_CLASS_NAME_PATTERN to callerClassName,
        CALLER_METHOD_NAME_PATTERN to callerMethodName,
        CALLER_LINE_NUMBER_PATTERN to callerLineNumber,
        CALLER_THREAD_NAME_PATTERN to callerThreadName,
      )
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
        every { loggerName } returns PLATFORM_LOGGER_NAME
        every { mdcPropertyMap } returns context
        every { threadName } returns logThreadName
        every { throwableProxy } returns null
        every { timeStamp } returns timestamp
      }

    val layout = AirbytePlatformLogbackMessageLayout()
    val message = layout.doLayout(loggingEvent = event)

    val expected =
      buildString {
        append("1970-01-01 00:00:00,000 ")
        append("[$callerThreadName]$TAB$ESC_START${ANSIConstants.BLUE_FG}$ESC_END${logLevel}$DEFAULT_COLOR$TAB")
        append("${formatClassName(callerClassName)}($callerMethodName):$callerLineNumber $DASH_CHAR ")
        append("$logMessage$LINE_SEPARATOR")
      }
    assertEquals(expected, message)
  }
}
