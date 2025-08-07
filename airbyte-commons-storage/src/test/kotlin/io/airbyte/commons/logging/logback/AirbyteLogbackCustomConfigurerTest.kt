/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging.logback

import ch.qos.logback.classic.Level
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
import io.airbyte.commons.logging.LOG_SOURCE_MDC_KEY
import io.airbyte.commons.logging.LogSource
import io.airbyte.commons.storage.DocumentType
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
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

  @Test
  fun testThresholdEvaluator() {
    val evaluator =
      AirbyteLogbackCustomConfigurer.ThresholdEvaluator().apply {
        threshold = Level.WARN
      }

    val errorEvent =
      mockk<ILoggingEvent> {
        every { level } returns Level.ERROR
      }
    val warnEvent =
      mockk<ILoggingEvent> {
        every { level } returns Level.WARN
      }
    val infoEvent =
      mockk<ILoggingEvent> {
        every { level } returns Level.INFO
      }
    val debugEvent =
      mockk<ILoggingEvent> {
        every { level } returns Level.DEBUG
      }

    assertFalse(evaluator.evaluate(errorEvent))
    assertFalse(evaluator.evaluate(warnEvent))
    assertTrue(evaluator.evaluate(infoEvent))
    assertTrue(evaluator.evaluate(debugEvent))
  }

  @Test
  fun testThresholdEvaluatorWithNullThreshold() {
    val evaluator = AirbyteLogbackCustomConfigurer.ThresholdEvaluator()

    val infoEvent =
      mockk<ILoggingEvent> {
        every { level } returns Level.INFO
      }

    assertFalse(evaluator.evaluate(infoEvent))
  }

  @Test
  fun testReplicationDebugEvaluator() {
    val loggerContext =
      mockk<LoggerContext> {
        every { getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME) } returns
          mockk {
            every { level } returns Level.DEBUG
          }
      }
    val evaluator =
      AirbyteLogbackCustomConfigurer.ReplicationDebugEvaluator().apply {
        context = loggerContext
      }

    val eventWithReplicationOrchestrator =
      mockk<ILoggingEvent> {
        every { mdcPropertyMap } returns mapOf(LOG_SOURCE_MDC_KEY to LogSource.REPLICATION_ORCHESTRATOR.displayName)
      }
    val eventWithDifferentSource =
      mockk<ILoggingEvent> {
        every { mdcPropertyMap } returns mapOf(LOG_SOURCE_MDC_KEY to "different-source")
      }
    val eventWithoutSource =
      mockk<ILoggingEvent> {
        every { mdcPropertyMap } returns emptyMap()
      }

    // Should allow (return false) when replication orchestrator and DEBUG level
    assertFalse(evaluator.evaluate(eventWithReplicationOrchestrator))
    // Should deny (return true) when different source
    assertTrue(evaluator.evaluate(eventWithDifferentSource))
    // Should deny (return true) when no source
    assertTrue(evaluator.evaluate(eventWithoutSource))
  }

  @Test
  fun testReplicationDebugEvaluatorWithInfoLevel() {
    val loggerContext =
      mockk<LoggerContext> {
        every { getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME) } returns
          mockk {
            every { level } returns Level.INFO
          }
      }
    val evaluator =
      AirbyteLogbackCustomConfigurer.ReplicationDebugEvaluator().apply {
        context = loggerContext
      }

    val eventWithReplicationOrchestrator =
      mockk<ILoggingEvent> {
        every { mdcPropertyMap } returns mapOf(LOG_SOURCE_MDC_KEY to LogSource.REPLICATION_ORCHESTRATOR.displayName)
      }

    // Should deny (return true) when replication orchestrator but INFO level (not DEBUG)
    assertTrue(evaluator.evaluate(eventWithReplicationOrchestrator))
  }

  @Test
  fun testCreateSiftingAppenderWithCustomEvaluator() {
    val loggerContext =
      mockk<LoggerContext> {
        every { getObject(any()) } returns mutableMapOf<String, String>()
        every { statusManager } returns
          mockk<StatusManager> {
            every { add(any<Status>()) } returns Unit
          }
        every { getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME) } returns
          mockk {
            every { level } returns Level.DEBUG
          }
      }
    val appenderFactory = mockk<AppenderFactory<ILoggingEvent>>()
    val customEvaluator =
      AirbyteLogbackCustomConfigurer.ReplicationDebugEvaluator().apply {
        context = loggerContext
      }

    val appender =
      configurer.createSiftingAppender(
        appenderFactory = appenderFactory,
        contextKey = "test-context-key",
        appenderName = "test-appender",
        loggerContext = loggerContext,
        evaluators = listOf(customEvaluator),
      )

    assertEquals(SiftingAppender::class.java, appender.javaClass)
    assertTrue(appender.isStarted)
  }

  @Test
  fun testCreateReplicationDumpAppender() {
    val loggerContext =
      mockk<LoggerContext> {
        every { getObject(any()) } returns mutableMapOf<String, String>()
        every { statusManager } returns
          mockk<StatusManager> {
            every { add(any<Status>()) } returns Unit
          }
        every { getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME) } returns
          mockk {
            every { level } returns Level.DEBUG
          }
      }

    val appender = configurer.createReplicationDumpAppender(loggerContext = loggerContext)

    assertEquals(SiftingAppender::class.java, appender.javaClass)
    assertEquals(loggerContext, appender.context)
    assertEquals(CLOUD_REPLICATION_JOB_DUMPER_NAME, appender.name)
    assertTrue(appender.isStarted)

    // Verify that the appender has multiple filters (MDC + Threshold + ReplicationDebug)
    val siftingAppender = appender as SiftingAppender
    val filterList = siftingAppender.getCopyOfAttachedFiltersList()
    assertEquals(3, filterList.size) // MDC key filter + Threshold filter + ReplicationDebug filter
  }

  @Test
  fun testCreateSiftingAppenderWithMultipleEvaluators() {
    val loggerContext =
      mockk<LoggerContext> {
        every { getObject(any()) } returns mutableMapOf<String, String>()
        every { statusManager } returns
          mockk<StatusManager> {
            every { add(any<Status>()) } returns Unit
          }
        every { getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME) } returns
          mockk {
            every { level } returns Level.DEBUG
          }
      }
    val appenderFactory = mockk<AppenderFactory<ILoggingEvent>>()

    val replicationDebugEvaluator =
      AirbyteLogbackCustomConfigurer.ReplicationDebugEvaluator().apply {
        context = loggerContext
      }

    val thresholdEvaluator =
      AirbyteLogbackCustomConfigurer.ThresholdEvaluator().apply {
        context = loggerContext
        threshold = Level.DEBUG
      }

    val appender =
      configurer.createSiftingAppender(
        appenderFactory = appenderFactory,
        contextKey = "test-context-key",
        appenderName = "test-appender",
        loggerContext = loggerContext,
        evaluators = listOf(replicationDebugEvaluator, thresholdEvaluator),
      )

    assertEquals(SiftingAppender::class.java, appender.javaClass)
    assertEquals(loggerContext, appender.context)
    assertTrue(appender.isStarted)
  }
}
