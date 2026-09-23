/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEvent
import ch.qos.logback.core.Appender
import ch.qos.logback.core.AppenderBase
import ch.qos.logback.core.read.ListAppender
import com.fasterxml.jackson.core.JsonProcessingException
import com.google.auth.oauth2.OAuth2CredentialsWithRefresh
import io.airbyte.api.client.ApiException
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.logging.logback.AirbyteFlexLogbackAppender
import io.airbyte.commons.logging.logback.CLOUD_OPERATIONS_JOB_LOGGER_NAME
import io.airbyte.commons.logging.logback.FLEX_OPERATIONS_JOB_LOGGER_NAME
import io.airbyte.commons.storage.CloudStorageBulkUploaderExecutor
import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.GcsLogUploadTarget
import io.airbyte.commons.storage.LogUploadAuthorizationRefreshResult
import io.airbyte.commons.storage.RefreshableGcsLogUploader
import io.airbyte.commons.storage.StorageClient
import io.airbyte.container.orchestrator.config.CommonBeanFactory
import io.airbyte.micronaut.runtime.AirbyteConnectorConfig
import io.airbyte.micronaut.runtime.AirbyteContextConfig
import io.airbyte.micronaut.runtime.StorageType
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.pod.FileConstants
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.domain.GcsDownscopedOAuthLogUploadAuthorization
import io.airbyte.workload.api.domain.LogUploadAuthorization
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkObject
import io.mockk.unmockkObject
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.time.OffsetDateTime
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

internal class FlexLogAppenderInitializerTest {
  @Test
  fun `missing blank malformed unsupported and wrong-type modes fall back to standard without logging mutation`(
    @TempDir configDir: Path,
  ) {
    listOf<Any?>(null, "", " ", "not-a-mode", "FUTURE", 42, true).forEach { mode ->
      writeReplicationMode(configDir, mode)
      val harness = harness(configDir)

      assertDoesNotThrow { harness.initializer.initialize() }

      assertSame(harness.standard, harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
      assertSame(harness.unrelated, harness.root.getAppender("unrelated"))
      assertFalse(harness.standard.stopped)
      verify(exactly = 0) { harness.workloadApiClient.workloadLogUploadAuthorization(any()) }
      verify(exactly = 0) { harness.workloadApiClient.workloadGet(any()) }
      harness.context.stop()
    }
  }

  @Test
  fun `replication input flex mode fetches initial authorization directly without handoff files`(
    @TempDir configDir: Path,
  ) {
    val inputPath = configDir.resolve(FileConstants.INIT_INPUT_FILE)
    Files.writeString(inputPath, """{"logDeliveryMode":"FLEX"}""")
    lateinit var flex: TrackingAppender
    val harness =
      harness(configDir, appenderFactory = { _, _, _, onStop ->
        TrackingAppender(onStop = onStop).also { flex = it }
      })
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns
      GcsDownscopedOAuthLogUploadAuthorization(
        "sensitive-token",
        OffsetDateTime.parse("2030-01-01T00:00:00Z"),
        "bucket",
        "job-logging/workload/",
      )

    harness.initializer.initialize()

    assertSame(flex, harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
    assertSame(harness.unrelated, harness.root.getAppender("unrelated"))
    verify(exactly = 1) { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) }
    verify(exactly = 0) { harness.workloadApiClient.workloadGet(any()) }
    assertFalse(Files.exists(configDir.resolve(MODE_FILE)))
    assertFalse(Files.exists(configDir.resolve(AUTH_FILE)))
    assertFalse(Files.readString(inputPath).contains("sensitive-token"))
  }

  @Test
  fun `explicit standard input ignores stale handoff files and leaves every appender untouched`(
    @TempDir configDir: Path,
  ) {
    writeReplicationMode(configDir, "STANDARD")
    Files.writeString(configDir.resolve(MODE_FILE), "\"FLEX\"")
    Files.writeString(configDir.resolve(AUTH_FILE), validAuthorizationJson())
    val harness = harness(configDir)

    harness.initializer.initialize()

    assertSame(harness.standard, harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
    assertSame(harness.unrelated, harness.root.getAppender("unrelated"))
    assertNull(harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
    verify(exactly = 0) { harness.workloadApiClient.workloadLogUploadAuthorization(any()) }
    verify(exactly = 0) { harness.workloadApiClient.workloadGet(any()) }
  }

  @Test
  fun `initial authorization API errors stay nonfatal and preserve standard delivery`(
    @TempDir configDir: Path,
  ) {
    listOf<Throwable>(
      IOException("sensitive-token and bucket"),
      ApiException(500, "sensitive-url", "sensitive-token"),
      ApiException(204, "sensitive-url", "sensitive-token"),
      object : JsonProcessingException("sensitive-token") {},
      AssertionError("sensitive-token and bucket"),
    ).forEach { failure ->
      writeReplicationMode(configDir, "FLEX")
      val harness = harness(configDir)
      every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } throws failure

      assertDoesNotThrow { harness.initializer.initialize() }

      assertSame(harness.standard, harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
      assertNull(harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
      assertSame(harness.unrelated, harness.root.getAppender("unrelated"))
      assertFalse(harness.standard.stopped)
      verify(exactly = 1) { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) }
      assertFalse(Files.readString(configDir.resolve(FileConstants.INIT_INPUT_FILE)).contains("sensitive-token"))
      harness.context.stop()
    }
  }

  @Test
  fun `flex without a valid initial authorization preserves standard remote delivery`(
    @TempDir configDir: Path,
  ) {
    listOf<LogUploadAuthorization?>(
      null,
      validAuthorization().copy(accessToken = ""),
      validAuthorization().copy(bucketName = ""),
      validAuthorization().copy(objectKeyPrefix = "invalid-prefix"),
    ).forEach { authorization ->
      writeReplicationMode(configDir, "FLEX")
      val harness = harness(configDir)
      every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns authorization

      assertDoesNotThrow { harness.initializer.initialize() }

      assertSame(harness.standard, harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
      assertNull(harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
      assertFalse(harness.standard.stopped)
      assertSame(harness.unrelated, harness.root.getAppender("unrelated"))
      verify(exactly = 1) { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) }
      harness.context.stop()
    }
  }

  @Test
  fun `valid flex replaces only standard operations appender`(
    @TempDir configDir: Path,
  ) {
    writeReplicationMode(configDir, "FLEX")
    lateinit var flex: TrackingAppender
    val harness =
      harness(configDir, appenderFactory = { _, _, _, onStop ->
        TrackingAppender(onStop = onStop).also { flex = it }
      })
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns validAuthorization()

    harness.initializer.initialize()

    assertNull(harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
    assertSame(flex, harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
    assertSame(harness.unrelated, harness.root.getAppender("unrelated"))
    assertTrue(harness.standard.isStarted)
    assertFalse(harness.standard.stopped)
    assertTrue(flex.isStarted)
    verify(exactly = 1) { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) }
    assertFalse(Files.exists(configDir.resolve(MODE_FILE)))
    assertFalse(Files.exists(configDir.resolve(AUTH_FILE)))

    harness.context.stop()
    assertTrue(harness.standard.stopped)
  }

  @Test
  fun `runtime storage failure detaches flex and restores usable standard delivery`(
    @TempDir configDir: Path,
  ) {
    writeReplicationMode(configDir, "FLEX")
    val diagnostics = mutableListOf<String>()
    val writes = AtomicInteger()
    lateinit var uploader: RefreshableGcsLogUploader<String>
    val harness =
      harness(
        configDir = configDir,
        appenderFactory = { target, refresh, onFailure, _ ->
          uploader =
            testUploader(
              initialTarget = target,
              refreshAuthorization = refresh,
              encode = { it.single() },
              onFailure = onFailure,
              storageClientFactory = { bucket, _ ->
                object : StorageClient {
                  override val bucketName = bucket
                  override val documentType = DocumentType.LOGS
                  override val storageType = StorageType.GCS

                  override fun write(
                    id: String,
                    document: String,
                  ) {
                    writes.incrementAndGet()
                    throw IOException("sensitive storage failure")
                  }

                  override fun list(id: String): List<String> = error("not used")

                  override fun read(id: String): String? = error("not used")

                  override fun delete(id: String): Boolean = error("not used")
                }
              },
            )
          TrackingAppender()
        },
        diagnostics = diagnostics::add,
      )
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns validAuthorization()
    harness.initializer.initialize()
    assertTrue(harness.standard.isStarted)
    assertNull(harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))

    runBlocking { uploader.append("first") }
    assertDoesNotThrow { uploader.flush() }
    runBlocking { uploader.append("ignored") }
    uploader.flush()
    val event = LoggingEvent()
    harness.root.callAppenders(event)

    assertEquals(1, writes.get())
    assertTrue(uploader.isDisabled)
    assertSame(harness.standard, harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
    assertTrue(harness.standard.isStarted)
    assertEquals(listOf(event), harness.standard.events)
    assertNull(harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
    assertEquals(listOf("$FLEX_FAILURE_MESSAGE Failure stage: FLEX runtime delivery."), diagnostics)
    assertTrue(diagnostics.none { it.contains("sensitive") })
    harness.context.stop()
  }

  @Test
  fun `failure building flex appender remains non-fatal and preserves standard delivery`(
    @TempDir configDir: Path,
  ) {
    writeReplicationMode(configDir, "FLEX")
    listOf<Throwable>(
      IOException("sensitive token and prefix"),
      AssertionError("sensitive token and prefix"),
    ).forEach { failure ->
      val harness = harness(configDir, appenderFactory = { _, _, _, _ -> throw failure })
      every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns validAuthorization()

      assertDoesNotThrow { harness.initializer.initialize() }

      assertSame(harness.standard, harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
      assertNull(harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
      assertSame(harness.unrelated, harness.root.getAppender("unrelated"))
      assertFalse(harness.standard.stopped)
      assertTrue(harness.standard.isStarted)
      val event = LoggingEvent()
      harness.root.callAppenders(event)
      assertEquals(listOf(event), harness.standard.events)
      harness.context.stop()
    }
  }

  @Test
  fun `periodic scheduler rejection from real flex appender preserves standard and reports installation failure`(
    @TempDir configDir: Path,
  ) {
    writeReplicationMode(configDir, "FLEX")
    val harness =
      harness(configDir, appenderFactory = {
        target,
        refresh,
        onFailure,
        onStop,
        ->
        AirbyteFlexLogbackAppender(target, refresh, onFailure, onStop)
      })
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns validAuthorization()
    mockkObject(CloudStorageBulkUploaderExecutor)
    try {
      every { CloudStorageBulkUploaderExecutor.scheduleTask(any(), any(), any(), any()) } throws
        RejectedExecutionException("sensitive token and prefix")

      val diagnostics = captureFlexDiagnostics { assertDoesNotThrow { harness.initializer.initialize() } }

      assertSame(harness.standard, harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
      assertTrue(harness.standard.isStarted)
      val event = LoggingEvent()
      harness.root.callAppenders(event)
      assertEquals(listOf(event), harness.standard.events)
      assertNull(harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
      assertEquals(listOf("$FLEX_FAILURE_MESSAGE Failure stage: FLEX appender installation."), diagnostics)
      assertTrue(diagnostics.none { it.contains("sensitive") })
    } finally {
      harness.context.stop()
      unmockkObject(CloudStorageBulkUploaderExecutor)
    }
  }

  @Test
  fun `interrupted authorization and installation failures restore interrupt status and preserve standard`(
    @TempDir configDir: Path,
  ) {
    writeReplicationMode(configDir, "FLEX")
    val authorizationHarness = harness(configDir)
    every { authorizationHarness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } throws
      InterruptedException("sensitive authorization")
    assertInterruptedFallback(authorizationHarness)

    val installationHarness = harness(configDir, appenderFactory = { _, _, _, _ -> throw InterruptedException("sensitive installation") })
    every { installationHarness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns validAuthorization()
    assertInterruptedFallback(installationHarness)
  }

  @Test
  fun `interrupted restoration never attaches a stopped standard appender`(
    @TempDir configDir: Path,
  ) {
    writeReplicationMode(configDir, "FLEX")
    val standard = TrackingAppender(restartFailure = InterruptedException("sensitive restoration"))
    val harness =
      harness(
        configDir,
        standard = standard,
        appenderFactory = { _, _, _, _ -> throw IOException("sensitive installation") },
      )
    standard.stop()
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns validAuthorization()

    try {
      assertDoesNotThrow { harness.initializer.initialize() }
      assertTrue(Thread.currentThread().isInterrupted)
      assertFalse(standard.isStarted)
      assertNull(harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
      assertNull(harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
    } finally {
      Thread.interrupted()
      harness.context.stop()
    }
  }

  @Test
  fun `virtual machine errors from authorization and installation propagate`(
    @TempDir configDir: Path,
  ) {
    writeReplicationMode(configDir, "FLEX")
    val authorizationFailure = OutOfMemoryError("sensitive authorization")
    val authorizationHarness = harness(configDir)
    every { authorizationHarness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } throws authorizationFailure
    assertSame(authorizationFailure, assertThrows(OutOfMemoryError::class.java) { authorizationHarness.initializer.initialize() })
    assertSame(authorizationHarness.standard, authorizationHarness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
    authorizationHarness.context.stop()

    val installationFailure = OutOfMemoryError("sensitive installation")
    val installationHarness = harness(configDir, appenderFactory = { _, _, _, _ -> throw installationFailure })
    every { installationHarness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns validAuthorization()
    assertSame(installationFailure, assertThrows(OutOfMemoryError::class.java) { installationHarness.initializer.initialize() })
    assertSame(installationHarness.standard, installationHarness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
    installationHarness.context.stop()
  }

  @Test
  fun `diagnostic assertion error is suppressed while standard remains attached`(
    @TempDir configDir: Path,
  ) {
    writeReplicationMode(configDir, "FLEX")
    val harness = harness(configDir, diagnostics = { throw AssertionError("sensitive diagnostic") })
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns null

    assertDoesNotThrow { harness.initializer.initialize() }

    assertSame(harness.standard, harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
    assertTrue(harness.standard.isStarted)
    harness.context.stop()
  }

  @Test
  fun `diagnostic interruption is suppressed and restores interrupt status`(
    @TempDir configDir: Path,
  ) {
    writeReplicationMode(configDir, "FLEX")
    val harness = harness(configDir, diagnostics = { throw InterruptedException("sensitive diagnostic") })
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns null

    try {
      assertDoesNotThrow { harness.initializer.initialize() }
      assertTrue(Thread.currentThread().isInterrupted)
      assertSame(harness.standard, harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
    } finally {
      Thread.interrupted()
      harness.context.stop()
    }
  }

  @Test
  fun `virtual machine error from diagnostics propagates while standard remains attached`(
    @TempDir configDir: Path,
  ) {
    writeReplicationMode(configDir, "FLEX")
    val failure = OutOfMemoryError("sensitive diagnostic")
    val harness = harness(configDir, diagnostics = { throw failure })
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns null

    assertSame(failure, assertThrows(OutOfMemoryError::class.java) { harness.initializer.initialize() })
    assertSame(harness.standard, harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
    harness.context.stop()
  }

  @Test
  fun `flex authorization with an invalid object prefix does not install an appender`(
    @TempDir configDir: Path,
  ) {
    writeReplicationMode(configDir, "FLEX")
    val harness = harness(configDir)
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns
      validAuthorization().copy(objectKeyPrefix = "other/workload")

    harness.initializer.initialize()

    assertSame(harness.standard, harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
    assertNull(harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
    assertFalse(harness.standard.stopped)
  }

  @Test
  fun `standard shutdown failure remains nonfatal during normal flex context shutdown`(
    @TempDir configDir: Path,
  ) {
    writeReplicationMode(configDir, "FLEX")
    listOf<Throwable>(
      IllegalStateException("sensitive stop failure"),
      AssertionError("sensitive stop failure"),
    ).forEach { failure ->
      val standard = TrackingAppender(stopFailure = failure)
      val diagnostics = mutableListOf<String>()
      lateinit var flex: TrackingAppender
      val harness =
        harness(
          configDir,
          standard = standard,
          appenderFactory = { _, _, _, onStop -> TrackingAppender(onStop = onStop).also { flex = it } },
          diagnostics = diagnostics::add,
        )
      every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns validAuthorization()

      assertDoesNotThrow { harness.initializer.initialize() }

      assertTrue(standard.isStarted)
      assertNull(harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
      assertSame(flex, harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
      assertDoesNotThrow { harness.context.stop() }
      assertTrue(standard.stopped)
      assertEquals(listOf("$FLEX_FAILURE_MESSAGE Failure stage: FLEX runtime delivery."), diagnostics)
    }
  }

  @Test
  fun `cleanup failure while recovering remains nonfatal and restores standard`(
    @TempDir configDir: Path,
  ) {
    writeReplicationMode(configDir, "FLEX")
    val standard = TrackingAppender()
    val flex =
      TrackingAppender(
        startFailure = IOException("sensitive flex startup failure"),
        stopFailure = AssertionError("sensitive flex cleanup failure"),
      )
    val harness = harness(configDir, standard = standard, appenderFactory = { _, _, _, _ -> flex })
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns validAuthorization()

    assertDoesNotThrow { harness.initializer.initialize() }

    assertSame(standard, harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
    assertTrue(standard.isStarted)
    assertNull(harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
    assertTrue(flex.stopped)
    harness.context.stop()
  }

  @Test
  fun `bootstrap diagnostics identify sanitized fixed failure stages`(
    @TempDir configDir: Path,
  ) {
    writeReplicationMode(configDir, "FLEX")
    val authorizationHarness = harness(configDir)
    every { authorizationHarness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } throws
      IOException("sensitive-token and bucket")

    val authorizationDiagnostics = captureFlexDiagnostics { authorizationHarness.initializer.initialize() }

    assertEquals(
      listOf("$FLEX_FAILURE_MESSAGE Failure stage: initial FLEX authorization."),
      authorizationDiagnostics,
    )
    assertTrue(authorizationDiagnostics.none { it.contains("sensitive") })
    assertSame(authorizationHarness.standard, authorizationHarness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
    assertFalse(authorizationHarness.standard.stopped)
    authorizationHarness.context.stop()

    val installationHarness = harness(configDir, appenderFactory = { _, _, _, _ -> throw IOException("sensitive-token and prefix") })
    every { installationHarness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns validAuthorization()

    val installationDiagnostics = captureFlexDiagnostics { installationHarness.initializer.initialize() }

    assertEquals(
      listOf("$FLEX_FAILURE_MESSAGE Failure stage: FLEX appender installation."),
      installationDiagnostics,
    )
    assertTrue(installationDiagnostics.none { it.contains("sensitive") })
    assertSame(installationHarness.standard, installationHarness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
    assertFalse(installationHarness.standard.stopped)
    installationHarness.context.stop()
  }

  @Test
  fun `refresh adapter retries only IO and 5xx and treats every other outcome as terminal`(
    @TempDir configDir: Path,
  ) {
    writeReplicationMode(configDir, "FLEX")
    lateinit var refresh: () -> LogUploadAuthorizationRefreshResult
    val harness =
      harness(configDir, appenderFactory = { _, callback, _, _ ->
        refresh = callback
        TrackingAppender()
      })
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns validAuthorization()
    harness.initializer.initialize()

    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns null
    assertSame(LogUploadAuthorizationRefreshResult.TerminalFailure, refresh())
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } throws ApiException(410, "url", "sensitive body")
    assertSame(LogUploadAuthorizationRefreshResult.TerminalFailure, refresh())
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } throws ApiException(300, "url", "sensitive body")
    assertSame(LogUploadAuthorizationRefreshResult.TerminalFailure, refresh())
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } throws IOException("sensitive token")
    assertSame(LogUploadAuthorizationRefreshResult.RetryableFailure, refresh())
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } throws ApiException(500, "url", "sensitive body")
    assertSame(LogUploadAuthorizationRefreshResult.RetryableFailure, refresh())
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } throws
      object : JsonProcessingException("sensitive malformed authorization") {}
    assertSame(LogUploadAuthorizationRefreshResult.TerminalFailure, refresh())
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } throws IllegalStateException("sensitive failure")
    assertSame(LogUploadAuthorizationRefreshResult.TerminalFailure, refresh())
  }

  @Test
  fun `status 200 empty response disables uploader and prevents later API and GCS calls`(
    @TempDir configDir: Path,
  ) {
    writeReplicationMode(configDir, "FLEX")
    lateinit var initialTarget: GcsLogUploadTarget
    lateinit var refresh: () -> LogUploadAuthorizationRefreshResult
    val harness =
      harness(configDir, appenderFactory = { target, callback, _, _ ->
        initialTarget = target
        refresh = callback
        TrackingAppender()
      })
    var uploaderForRefresh: RefreshableGcsLogUploader<String>? = null
    lateinit var credentials: OAuth2CredentialsWithRefresh
    val gcsCalls = AtomicInteger()
    val diagnostics = mutableListOf<String>()
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns validAuthorization()
    harness.initializer.initialize()
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } answers {
      runBlocking { uploaderForRefresh!!.append("queued-during-refresh") }
      throw ApiException(200, "sensitive-url", "sensitive-body")
    }
    val uploader =
      testUploader(
        initialTarget = initialTarget,
        refreshAuthorization = refresh,
        encode = { it.single() },
        onFailure = diagnostics::add,
        storageClientFactory = { bucket, createdCredentials ->
          credentials = createdCredentials
          object : StorageClient {
            override val bucketName = bucket
            override val documentType = DocumentType.LOGS
            override val storageType = StorageType.GCS

            override fun write(
              id: String,
              document: String,
            ) {
              gcsCalls.incrementAndGet()
              credentials.refresh()
            }

            override fun list(id: String): List<String> = error("not used")

            override fun read(id: String): String? = error("not used")

            override fun delete(id: String): Boolean = error("not used")
          }
        },
      )
    uploaderForRefresh = uploader

    runBlocking { uploader.append("current") }
    uploader.flush()

    assertTrue(uploader.isDisabled)
    assertEquals(0, uploader.queuedEventCount())
    runBlocking { uploader.append("ignored") }
    uploader.flush()
    assertThrows(IOException::class.java) { credentials.refresh() }
    assertEquals(1, gcsCalls.get())
    verify(exactly = 2) { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) }
    assertEquals(1, diagnostics.size)
    assertTrue(diagnostics.none { it.contains("sensitive") || it.contains("token") || it.contains("job-logging") })
  }

  @Suppress("UNCHECKED_CAST")
  private fun <T : Any> testUploader(
    initialTarget: GcsLogUploadTarget,
    refreshAuthorization: () -> LogUploadAuthorizationRefreshResult,
    encode: (List<T>) -> String,
    onFailure: (String) -> Unit,
    storageClientFactory: (String, OAuth2CredentialsWithRefresh) -> StorageClient,
  ): RefreshableGcsLogUploader<T> {
    // The fake-client constructor is internal to commons-storage but public in JVM bytecode.
    val storageClientConstructor =
      RefreshableGcsLogUploader::class.java.constructors.single {
        it.parameterCount == 9 && it.parameterTypes.last() == Function2::class.java
      }
    return storageClientConstructor.newInstance(
      initialTarget,
      refreshAuthorization,
      encode,
      60L,
      TimeUnit.SECONDS,
      10_000,
      onFailure,
      CloudStorageBulkUploaderExecutor::executeTask,
      storageClientFactory,
    ) as RefreshableGcsLogUploader<T>
  }

  private fun RefreshableGcsLogUploader<*>.queuedEventCount(): Int {
    val bufferField = RefreshableGcsLogUploader::class.java.getDeclaredField("buffer").apply { isAccessible = true }
    return (bufferField.get(this) as Collection<*>).size
  }

  private fun harness(
    configDir: Path,
    standard: TrackingAppender = TrackingAppender(),
    appenderFactory: (
      GcsLogUploadTarget,
      () -> LogUploadAuthorizationRefreshResult,
      (String) -> Unit,
      () -> Unit,
    ) -> Appender<ILoggingEvent> = { _, _, _, _ -> TrackingAppender() },
    diagnostics: ((String) -> Unit)? = null,
  ): Harness {
    val context = LoggerContext()
    val root = context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
    standard.apply {
      name = CLOUD_OPERATIONS_JOB_LOGGER_NAME
      start()
    }
    val unrelated =
      TrackingAppender().apply {
        name = "unrelated"
        start()
      }
    root.addAppender(standard)
    root.addAppender(unrelated)
    val workloadApiClient = mockk<WorkloadApiClient>(relaxed = true)
    val connectorConfig = AirbyteConnectorConfig(configDir = configDir.toString())
    val replicationInput =
      if (Files.exists(configDir.resolve(FileConstants.INIT_INPUT_FILE))) {
        CommonBeanFactory().replicationInput(connectorConfig)
      } else {
        ReplicationInput()
      }
    val initializer =
      if (diagnostics == null) {
        FlexLogAppenderInitializer(
          replicationInput = replicationInput,
          contextConfig = AirbyteContextConfig(workloadId = WORKLOAD_ID),
          workloadApiClient = workloadApiClient,
          loggerContextProvider = { context },
          appenderFactory = appenderFactory,
        )
      } else {
        FlexLogAppenderInitializer(
          replicationInput = replicationInput,
          contextConfig = AirbyteContextConfig(workloadId = WORKLOAD_ID),
          workloadApiClient = workloadApiClient,
          loggerContextProvider = { context },
          appenderFactory = appenderFactory,
          diagnostics = diagnostics,
        )
      }
    return Harness(context, root, standard, unrelated, workloadApiClient, initializer)
  }

  private fun validAuthorizationJson(): String =
    """{"type":"GCS_DOWNSCOPED_OAUTH","accessToken":"token","expiresAt":"2030-01-01T00:00:00Z","bucketName":"bucket","objectKeyPrefix":"job-logging/workload/"}"""

  private fun validAuthorization(): GcsDownscopedOAuthLogUploadAuthorization =
    GcsDownscopedOAuthLogUploadAuthorization(
      "token",
      OffsetDateTime.parse("2030-01-01T00:00:00Z"),
      "bucket",
      "job-logging/workload/",
    )

  private fun captureFlexDiagnostics(action: () -> Unit): List<String> {
    val context = LoggerFactory.getILoggerFactory() as LoggerContext
    val root = context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
    val appender = ListAppender<ILoggingEvent>().apply { start() }
    root.addAppender(appender)
    try {
      action()
    } finally {
      root.detachAppender(appender)
      appender.stop()
    }
    return appender.list.map { it.formattedMessage }.filter { it.startsWith("Unable to initialize FLEX log delivery.") }
  }

  private fun assertInterruptedFallback(harness: Harness) {
    try {
      assertDoesNotThrow { harness.initializer.initialize() }
      assertTrue(Thread.currentThread().isInterrupted)
      assertSame(harness.standard, harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
      assertTrue(harness.standard.isStarted)
      val event = LoggingEvent()
      harness.root.callAppenders(event)
      assertEquals(listOf(event), harness.standard.events)
      assertNull(harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
    } finally {
      Thread.interrupted()
      harness.context.stop()
    }
  }

  private fun writeReplicationMode(
    configDir: Path,
    mode: Any?,
  ) {
    val input = ReplicationInput()
    mode?.let { input.setAdditionalProperty("logDeliveryMode", it) }
    Files.writeString(configDir.resolve(FileConstants.INIT_INPUT_FILE), Jsons.serialize(input))
  }

  private data class Harness(
    val context: LoggerContext,
    val root: ch.qos.logback.classic.Logger,
    val standard: TrackingAppender,
    val unrelated: TrackingAppender,
    val workloadApiClient: WorkloadApiClient,
    val initializer: FlexLogAppenderInitializer,
  )

  private class TrackingAppender(
    private val onStop: () -> Unit = {},
    private val startFailure: Throwable? = null,
    private var stopFailure: Throwable? = null,
    private var restartFailure: Throwable? = null,
  ) : AppenderBase<ILoggingEvent>() {
    val events = mutableListOf<ILoggingEvent>()
    var stopped = false
    private var startCount = 0

    override fun append(eventObject: ILoggingEvent) {
      events += eventObject
    }

    override fun stop() {
      stopped = true
      onStop()
      super.stop()
      stopFailure?.let {
        stopFailure = null
        throw it
      }
    }

    override fun start() {
      startCount += 1
      if (startCount > 1) {
        restartFailure?.let {
          restartFailure = null
          throw it
        }
      }
      stopped = false
      super.start()
      startFailure?.let { throw it }
    }
  }

  private companion object {
    const val FLEX_FAILURE_MESSAGE = "Unable to initialize FLEX log delivery. Continuing with STANDARD operations-log delivery."
    const val MODE_FILE = "log-delivery-mode.json"
    const val AUTH_FILE = "log-upload-authorization.json"
    const val WORKLOAD_ID = "workload-id"
  }
}
