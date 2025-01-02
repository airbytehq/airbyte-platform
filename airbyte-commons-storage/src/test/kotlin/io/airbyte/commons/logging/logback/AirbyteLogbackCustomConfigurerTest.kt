/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging.logback

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.sift.SiftingAppender
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Context
import ch.qos.logback.core.OutputStreamAppender
import ch.qos.logback.core.encoder.LayoutWrappingEncoder
import ch.qos.logback.core.sift.AppenderFactory
import ch.qos.logback.core.status.Status
import ch.qos.logback.core.status.StatusManager
import ch.qos.logback.core.util.Duration
import io.airbyte.commons.storage.DocumentType
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private class AirbyteLogbackCustomConfigurerTest {
  private lateinit var configurer: AirbyteLogbackCustomConfigurer

  @BeforeEach
  fun setUp() {
    configurer = AirbyteLogbackCustomConfigurer()
  }

  @Test
  fun testCreatePlatformConsoleAppender() {
    val context =
      mockk<LoggerContext> {
        every { getObject(any()) } returns mutableMapOf<String, String>()
        every { statusManager } returns
          mockk<StatusManager> {
            every { add(any<Status>()) } returns Unit
          }
      }
    val appender = configurer.createPlatformAppender(loggerContext = context)
    assertEquals(context, appender.context)
    assertEquals(PLATFORM_LOGGER_NAME, appender.name)
    assertEquals(
      AirbytePlatformLogbackMessageLayout::class.java,
      ((appender as OutputStreamAppender<ILoggingEvent>).encoder as LayoutWrappingEncoder).layout.javaClass,
    )

    assertTrue(appender.isStarted)
  }

  @Test
  fun testCreateAirbyteCloudStorageAppender() {
    val context =
      mockk<Context> {
        every { getObject(any()) } returns mutableMapOf<String, String>()
        every { statusManager } returns
          mockk<StatusManager> {
            every { add(any<Status>()) } returns Unit
          }
      }
    val appenderName = "test-appender"
    val discriminatorValue = "/workspace/1"
    val documentType = DocumentType.LOGS
    val appender =
      configurer.createCloudAppender(
        context = context,
        discriminatorValue = discriminatorValue,
        documentType = documentType,
        appenderName = appenderName,
      )

    assertEquals(AirbyteCloudStorageAppender::class.java, appender.javaClass)
    assertEquals(context, appender.context)
    assertEquals("$appenderName-$discriminatorValue", appender.name)
    assertEquals(documentType, appender.documentType)
    assertEquals(discriminatorValue, appender.baseStorageId)

    assertTrue(appender.isStarted)
  }

  @Test
  fun testCreateSiftingAppender() {
    val loggerContext =
      mockk<LoggerContext> {
        every { getObject(any()) } returns mutableMapOf<String, String>()
        every { statusManager } returns
          mockk<StatusManager> {
            every { add(any<Status>()) } returns Unit
          }
      }
    val appenderFactory = mockk<AppenderFactory<ILoggingEvent>>()
    val appenderName = "test-appender"
    val contextKey = "test-context-key"
    val appender =
      configurer.createSiftingAppender(
        appenderFactory = appenderFactory,
        contextKey = contextKey,
        appenderName = appenderName,
        loggerContext = loggerContext,
      )

    assertEquals(SiftingAppender::class.java, appender.javaClass)
    assertEquals(loggerContext, appender.context)
    assertEquals(appenderName, appender.name)
    assertEquals(Duration.valueOf("$APPENDER_TIMEOUT minutes").milliseconds, appender.timeout.milliseconds)

    assertTrue(appender.isStarted)
  }
}
