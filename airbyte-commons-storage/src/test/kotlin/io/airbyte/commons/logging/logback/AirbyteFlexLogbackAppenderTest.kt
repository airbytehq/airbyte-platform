/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging.logback

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.ILoggingEvent
import io.airbyte.commons.constants.AirbyteSecretConstants.SECRETS_MASK
import io.airbyte.commons.logging.DEFAULT_JOB_LOG_PATH_MDC_KEY
import io.airbyte.commons.logging.LOG_SOURCE_MDC_KEY
import io.airbyte.commons.logging.LogSource
import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.GcsLogUploadTarget
import io.airbyte.commons.storage.LogUploadAuthorizationRefreshResult
import io.airbyte.commons.storage.RefreshableGcsLogUploader
import io.airbyte.commons.storage.StorageClient
import io.airbyte.micronaut.runtime.StorageType
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

internal class AirbyteFlexLogbackAppenderTest {
  @Test
  fun `threshold upload never blocks the triggering or subsequent doAppend caller`() {
    val writeStarted = CountDownLatch(1)
    val releaseWrite = CountDownLatch(1)
    val writes = AtomicInteger()
    val storageClient =
      object : StorageClient {
        override val bucketName = "bucket"
        override val documentType = DocumentType.LOGS
        override val storageType = StorageType.GCS

        override fun write(
          id: String,
          document: String,
        ) {
          writes.incrementAndGet()
          writeStarted.countDown()
          releaseWrite.await()
        }

        override fun list(id: String): List<String> = error("not used")

        override fun read(id: String): String? = error("not used")

        override fun delete(id: String): Boolean = error("not used")
      }
    val encoder = AirbyteLogEventEncoder().apply { start() }
    val uploader =
      RefreshableGcsLogUploader<ILoggingEvent>(
        initialTarget = target(),
        refreshAuthorization = { LogUploadAuthorizationRefreshResult.TerminalFailure },
        encode = encoder::bulkEncode,
        flushSize = 1,
        storageClientFactory = { _, _ -> storageClient },
      )
    val appender = AirbyteFlexLogbackAppender(uploader, encoder)
    val callersCompleted = CountDownLatch(2)
    val executor = Executors.newFixedThreadPool(2)
    appender.start()

    val triggeringAppend =
      executor.submit {
        try {
          appender.doAppend(event(Level.INFO, "trigger", hasJobMdc = true))
        } finally {
          callersCompleted.countDown()
        }
      }
    try {
      assertTrue(writeStarted.await(5, TimeUnit.SECONDS))
      val subsequentAppend =
        executor.submit {
          try {
            appender.doAppend(event(Level.INFO, "subsequent", hasJobMdc = true))
          } finally {
            callersCompleted.countDown()
          }
        }

      assertTrue(
        callersCompleted.await(1, TimeUnit.SECONDS),
        "both doAppend calls must return while the threshold upload remains blocked",
      )
      assertEquals(1L, releaseWrite.count)
      assertEquals(1, writes.get())
      triggeringAppend.get(5, TimeUnit.SECONDS)
      subsequentAppend.get(5, TimeUnit.SECONDS)
    } finally {
      releaseWrite.countDown()
      triggeringAppend.get(5, TimeUnit.SECONDS)
      appender.stop()
      executor.shutdownNow()
    }
  }

  @Test
  fun `uploads only info events with job MDC using structured masking`() {
    val documents = mutableListOf<String>()
    val storageClient =
      object : StorageClient {
        override val bucketName = "bucket"
        override val documentType = DocumentType.LOGS
        override val storageType = StorageType.GCS

        override fun write(
          id: String,
          document: String,
        ) {
          documents += document
        }

        override fun list(id: String): List<String> = error("not used")

        override fun read(id: String): String? = error("not used")

        override fun delete(id: String): Boolean = error("not used")
      }
    val encoder = AirbyteLogEventEncoder().apply { start() }
    val uploader =
      RefreshableGcsLogUploader<ILoggingEvent>(
        initialTarget =
          target(),
        refreshAuthorization = { LogUploadAuthorizationRefreshResult.TerminalFailure },
        encode = encoder::bulkEncode,
        storageClientFactory = { _, _ -> storageClient },
      )
    val appender = AirbyteFlexLogbackAppender(uploader, encoder)
    appender.start()

    appender.doAppend(event(Level.DEBUG, "debug apikey=debug-secret", hasJobMdc = true))
    appender.doAppend(event(Level.INFO, "missing apikey=missing-secret", hasJobMdc = false))
    appender.doAppend(event(Level.INFO, "visible apikey=secret-key", hasJobMdc = true))
    appender.stop()

    assertEquals(1, documents.size)
    assertTrue(documents.single().contains("visible apikey=$SECRETS_MASK"))
    assertFalse(documents.single().contains("secret-key"))
    assertFalse(documents.single().contains("debug-secret"))
    assertFalse(documents.single().contains("missing-secret"))
  }

  private fun event(
    level: Level,
    message: String,
    hasJobMdc: Boolean,
  ): ILoggingEvent =
    mockk {
      every { callerData } returns emptyArray()
      every { formattedMessage } returns message
      every { this@mockk.level } returns level
      every { loggerName } returns "test"
      every { mdcPropertyMap } returns
        buildMap {
          put(LOG_SOURCE_MDC_KEY, LogSource.REPLICATION_ORCHESTRATOR.displayName)
          if (hasJobMdc) put(DEFAULT_JOB_LOG_PATH_MDC_KEY, "job/path")
        }
      every { threadName } returns "test-thread"
      every { throwableProxy } returns null
      every { timeStamp } returns 1L
    }

  private fun target(): GcsLogUploadTarget =
    GcsLogUploadTarget(
      accessToken = "token",
      expiresAt = OffsetDateTime.parse("2030-01-01T00:00:00Z"),
      bucketName = "bucket",
      objectKeyPrefix = "job-logging/workload/",
    )
}
