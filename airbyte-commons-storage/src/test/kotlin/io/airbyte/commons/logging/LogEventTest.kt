/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.LoggingEvent
import ch.qos.logback.classic.spi.ThrowableProxy
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.kotlin.readValue
import io.airbyte.commons.constants.AirbyteSecretConstants.SECRETS_MASK
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.commons.logging.logback.CALLER_LINE_NUMBER_PATTERN
import io.airbyte.commons.logging.logback.CALLER_METHOD_NAME_PATTERN
import io.airbyte.commons.logging.logback.CALLER_QUALIFIED_CLASS_NAME_PATTERN
import io.airbyte.commons.logging.logback.CALLER_THREAD_NAME_PATTERN
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.UUID

internal class LogEventTest {
  @Test
  fun testToLogEvent() {
    val callerClassName = "io.airbyte.test.Test"
    val callerFileName = "Test.kt"
    val callerMethodName = "testMethod"
    val callerLineNumber = 123
    val callerThreadName = "thread-name-1"
    val logLevel = Level.INFO
    val logMessage = "This is a test message"
    val throwable = NullPointerException()
    val timestamp = System.currentTimeMillis()
    val loggingEvent =
      mockk<LoggingEvent> {
        every { callerData } returns arrayOf(StackTraceElement(callerClassName, callerMethodName, callerFileName, callerLineNumber))
        every { formattedMessage } returns logMessage
        every { level } returns logLevel
        every { mdcPropertyMap } returns emptyMap()
        every { message } returns logMessage
        every { threadName } returns callerThreadName
        every { timeStamp } returns timestamp
        every { throwableProxy } returns ThrowableProxy(throwable)
      }

    val logEvent = loggingEvent.toLogEvent()
    assertEquals(timestamp, logEvent.timestamp)
    assertEquals(logMessage, logEvent.message)
    assertEquals(logLevel.toString(), logEvent.level)
    assertEquals(throwable, logEvent.throwable)
    assertEquals(LogSource.PLATFORM, logEvent.logSource)
    assertEquals(callerThreadName, logEvent.caller?.threadName)
    assertEquals(callerClassName, logEvent.caller?.className)
    assertEquals(callerMethodName, logEvent.caller?.methodName)
    assertEquals(callerLineNumber, logEvent.caller?.lineNumber)
  }

  @Test
  fun testToLogEventMaskedData() {
    val callerClassName = "io.airbyte.test.Test"
    val callerFileName = "Test.kt"
    val callerMethodName = "testMethod"
    val callerLineNumber = 123
    val callerThreadName = "thread-name-1"
    val logLevel = Level.INFO
    val apiKey = UUID.randomUUID().toString()
    val logMessage = "test message (\"api_token\":\"$apiKey\")"
    val throwable = NullPointerException()
    val timestamp = System.currentTimeMillis()
    val loggingEvent =
      mockk<LoggingEvent> {
        every { callerData } returns arrayOf(StackTraceElement(callerClassName, callerMethodName, callerFileName, callerLineNumber))
        every { formattedMessage } returns logMessage
        every { level } returns logLevel
        every { mdcPropertyMap } returns emptyMap()
        every { message } returns logMessage
        every { threadName } returns callerThreadName
        every { timeStamp } returns timestamp
        every { throwableProxy } returns ThrowableProxy(throwable)
      }

    val logEvent = loggingEvent.toLogEvent()
    assertEquals(timestamp, logEvent.timestamp)
    assertEquals(logMessage.replace(apiKey, SECRETS_MASK), logEvent.message)
    assertEquals(logLevel.toString(), logEvent.level)
    assertEquals(throwable, logEvent.throwable)
    assertEquals(LogSource.PLATFORM, logEvent.logSource)
    assertEquals(callerThreadName, logEvent.caller?.threadName)
    assertEquals(callerClassName, logEvent.caller?.className)
    assertEquals(callerMethodName, logEvent.caller?.methodName)
    assertEquals(callerLineNumber, logEvent.caller?.lineNumber)
  }

  @Test
  fun testToLogEventWithMdc() {
    val callerClassName = "io.airbyte.test.Test"
    val callerFileName = "Test.kt"
    val callerMethodName = "testMethod"
    val callerLineNumber = 123
    val callerThreadName = "thread-name-1"
    val expectedCallerClassName = "io.airbyte.test.TestExpected"
    val expectedCallerMethodName = "testExpectedMethod"
    val expectedCallerLineNumber = 544
    val expectedCallerThreadName = "thread-name-expected-1"
    val logLevel = Level.INFO
    val logMessage = "This is a test message"
    val logSource = LogSource.SOURCE
    val mdc =
      mapOf(
        CALLER_QUALIFIED_CLASS_NAME_PATTERN to expectedCallerClassName,
        CALLER_METHOD_NAME_PATTERN to expectedCallerMethodName,
        CALLER_LINE_NUMBER_PATTERN to expectedCallerLineNumber.toString(),
        CALLER_THREAD_NAME_PATTERN to expectedCallerThreadName,
        LOG_SOURCE_MDC_KEY to logSource.displayName,
      )
    val throwable = NullPointerException()
    val timestamp = System.currentTimeMillis()
    val loggingEvent =
      mockk<LoggingEvent> {
        every { callerData } returns arrayOf(StackTraceElement(callerClassName, callerMethodName, callerFileName, callerLineNumber))
        every { formattedMessage } returns logMessage
        every { level } returns logLevel
        every { mdcPropertyMap } returns mdc
        every { message } returns logMessage
        every { threadName } returns callerThreadName
        every { timeStamp } returns timestamp
        every { throwableProxy } returns ThrowableProxy(throwable)
      }

    val logEvent = loggingEvent.toLogEvent()
    assertEquals(timestamp, logEvent.timestamp)
    assertEquals(logMessage, logEvent.message)
    assertEquals(logLevel.toString(), logEvent.level)
    assertEquals(throwable, logEvent.throwable)
    assertEquals(logSource, logEvent.logSource)
    assertEquals(expectedCallerThreadName, logEvent.caller?.threadName)
    assertEquals(expectedCallerClassName, logEvent.caller?.className)
    assertEquals(expectedCallerMethodName, logEvent.caller?.methodName)
    assertEquals(expectedCallerLineNumber, logEvent.caller?.lineNumber)
  }

  @Test
  fun testToCallerData() {
    val callerClassName = "io.airbyte.test.Test"
    val callerFileName = "Test.kt"
    val callerMethodName = "testMethod"
    val callerLineNumber = 123
    val callerThreadName = "thread-name-1"
    val logLevel = Level.INFO
    val logMessage = "This is a test message"
    val throwable = NullPointerException()
    val timestamp = System.currentTimeMillis()
    val loggingEvent =
      mockk<LoggingEvent> {
        every { callerData } returns arrayOf(StackTraceElement(callerClassName, callerMethodName, callerFileName, callerLineNumber))
        every { formattedMessage } returns logMessage
        every { level } returns logLevel
        every { mdcPropertyMap } returns emptyMap()
        every { message } returns logMessage
        every { threadName } returns callerThreadName
        every { timeStamp } returns timestamp
        every { throwableProxy } returns ThrowableProxy(throwable)
      }

    val callerData = loggingEvent.getCaller()
    assertEquals(callerClassName, callerData.className)
    assertEquals(callerMethodName, callerData.methodName)
    assertEquals(callerLineNumber, callerData.lineNumber)
    assertEquals(callerThreadName, callerData.threadName)
  }

  @Test
  fun testToCallerDataWithMdc() {
    val callerClassName = "io.airbyte.test.Test"
    val callerFileName = "Test.kt"
    val callerMethodName = "testMethod"
    val callerLineNumber = 123
    val callerThreadName = "thread-name-1"
    val expectedCallerClassName = "io.airbyte.test.TestExpected"
    val expectedCallerMethodName = "testExpectedMethod"
    val expectedCallerLineNumber = 544
    val expectedCallerThreadName = "thread-name-expected-1"
    val logLevel = Level.INFO
    val logMessage = "This is a test message"
    val mdc =
      mapOf(
        CALLER_QUALIFIED_CLASS_NAME_PATTERN to expectedCallerClassName,
        CALLER_METHOD_NAME_PATTERN to expectedCallerMethodName,
        CALLER_LINE_NUMBER_PATTERN to expectedCallerLineNumber.toString(),
        CALLER_THREAD_NAME_PATTERN to expectedCallerThreadName,
      )
    val throwable = NullPointerException()
    val timestamp = System.currentTimeMillis()
    val loggingEvent =
      mockk<LoggingEvent> {
        every { callerData } returns arrayOf(StackTraceElement(callerClassName, callerMethodName, callerFileName, callerLineNumber))
        every { formattedMessage } returns logMessage
        every { level } returns logLevel
        every { mdcPropertyMap } returns mdc
        every { message } returns logMessage
        every { threadName } returns callerThreadName
        every { timeStamp } returns timestamp
        every { throwableProxy } returns ThrowableProxy(throwable)
      }

    val callerData = loggingEvent.getCaller()
    assertEquals(expectedCallerClassName, callerData.className)
    assertEquals(expectedCallerMethodName, callerData.methodName)
    assertEquals(expectedCallerLineNumber, callerData.lineNumber)
    assertEquals(expectedCallerThreadName, callerData.threadName)
  }

  @Test
  fun testSerializationDeserialization() {
    val module = SimpleModule()
    module.addDeserializer(StackTraceElement::class.java, StackTraceElementDeserializer())
    module.addSerializer(StackTraceElement::class.java, StackTraceElementSerializer())
    val objectMapper = MoreMappers.initMapper()
    objectMapper.registerModules(module)

    val callerClassName = "io.airbyte.test.Test"
    val callerFileName = "Test.kt"
    val callerMethodName = "testMethod"
    val callerLineNumber = 123
    val callerThreadName = "thread-name-1"
    val logLevel = Level.INFO
    val logMessage = "This is a test message"
    val throwable = NullPointerException()
    val timestamp = System.currentTimeMillis()
    val loggingEvent =
      mockk<LoggingEvent> {
        every { callerData } returns arrayOf(StackTraceElement(callerClassName, callerMethodName, callerFileName, callerLineNumber))
        every { formattedMessage } returns logMessage
        every { level } returns logLevel
        every { mdcPropertyMap } returns emptyMap()
        every { message } returns logMessage
        every { threadName } returns callerThreadName
        every { timeStamp } returns timestamp
        every { throwableProxy } returns ThrowableProxy(throwable)
      }

    val logEvent = loggingEvent.toLogEvent()
    val logEvents = LogEvents(events = listOf(logEvent))
    val json = objectMapper.writeValueAsString(logEvents)
    assertTrue(json.isNotBlank())
    val obj = objectMapper.readValue<LogEvents>(json)
    assertNotNull(obj)
    assertEquals(logEvents.events.size, obj.events.size)
    assertEquals(LOG_EVENT_SCHEMA_VERSION, logEvents.version)
    assertEquals(logEvents.events.first().logSource, obj.events.first().logSource)
    assertEquals(logEvents.events.first().level, obj.events.first().level)
    assertEquals(logEvents.events.first().caller, obj.events.first().caller)
    assertEquals(logEvents.events.first().message, obj.events.first().message)
    assertEquals(logEvents.events.first().timestamp, obj.events.first().timestamp)
    logEvents.events.first().throwable?.stackTrace?.forEachIndexed { index, expected ->
      assertEquals(expected.className, obj.events.first().throwable?.stackTrace?.get(index)?.className)
      assertEquals(expected.methodName, obj.events.first().throwable?.stackTrace?.get(index)?.methodName)
      assertEquals(expected.lineNumber, obj.events.first().throwable?.stackTrace?.get(index)?.lineNumber)
    }
  }
}
