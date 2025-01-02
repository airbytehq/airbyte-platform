/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging.logback

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.ThrowableProxy
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.commons.logging.LogEvent
import io.airbyte.commons.logging.LogSource
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class AirbyteLogEventEncoderTest {
  private lateinit var encoder: AirbyteLogEventEncoder

  @BeforeEach
  fun setUp() {
    encoder = AirbyteLogEventEncoder()
  }

  @Test
  fun testHeaderBytes() {
    assertEquals(EMPTY_BYTES, encoder.headerBytes())
  }

  @Test
  fun testFooterBytes() {
    assertEquals(EMPTY_BYTES, encoder.footerBytes())
  }

  @Test
  fun testConvertingLoggingEventToStructuredEvent() {
    val objectMapper = MoreMappers.initMapper()
    val className = "io.airbyte.TestClass"
    val context = emptyMap<String, String>()
    val methodName = "testMethod"
    val fileName = "TestClass.kt"
    val lineNumber = 12345
    val logLevel = Level.INFO
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

    val structuredBytes = encoder.encode(loggingEvent = event)
    assertEquals(NEW_LINE.last(), structuredBytes.last())
    val structuredEvent = objectMapper.readValue(structuredBytes, LogEvent::class.java)
    assertEquals(LogSource.PLATFORM, structuredEvent.logSource)
    assertEquals(logLevel.toString(), structuredEvent.level)
    assertEquals(logMessage, structuredEvent.message)
    assertEquals(methodName, structuredEvent.caller?.methodName)
    assertEquals(className, structuredEvent.caller?.className)
    assertEquals(lineNumber, structuredEvent.caller?.lineNumber)
    assertEquals(logThreadName, structuredEvent.caller?.threadName)
    assertEquals(exception.message, structuredEvent.throwable?.message)
    assertEquals(timestamp, structuredEvent.timestamp)
  }
}
