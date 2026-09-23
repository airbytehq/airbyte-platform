/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorSidecar

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEvent
import ch.qos.logback.core.Appender
import ch.qos.logback.core.AppenderBase
import com.fasterxml.jackson.core.JsonProcessingException
import com.google.auth.oauth2.OAuth2CredentialsWithRefresh
import io.airbyte.api.client.ApiException
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.logging.DEFAULT_JOB_LOG_PATH_MDC_KEY
import io.airbyte.commons.logging.logback.AUDIT_LOGGER_NAME
import io.airbyte.commons.logging.logback.AirbyteFlexLogbackAppender
import io.airbyte.commons.logging.logback.CLOUD_OPERATIONS_JOB_LOGGER_NAME
import io.airbyte.commons.logging.logback.CLOUD_REPLICATION_JOB_DUMPER_NAME
import io.airbyte.commons.logging.logback.FLEX_OPERATIONS_JOB_LOGGER_NAME
import io.airbyte.commons.storage.CloudStorageBulkUploaderExecutor
import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.GcsLogUploadTarget
import io.airbyte.commons.storage.LogUploadAuthorizationRefreshResult
import io.airbyte.commons.storage.RefreshableGcsLogUploader
import io.airbyte.commons.storage.StorageClient
import io.airbyte.micronaut.runtime.StorageType
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.workers.models.SidecarInput
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.domain.GcsDownscopedOAuthLogUploadAuthorization
import io.airbyte.workload.api.domain.LogDeliveryMode
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
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.io.IOException
import java.time.OffsetDateTime
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

internal class FlexLogAppenderInitializerTest {
  @Test
  fun `missing blank malformed and unsupported modes fall back to standard without logging mutation`() {
    val missingModeInput =
      Jsons.deserialize(
        """{"checkConnectionInput":null,"discoverCatalogInput":null,"workloadId":"workload-id","integrationLauncherConfig":{},"operationType":"CHECK","logPath":"job-logging/workload/"}""",
        SidecarInput::class.java,
      )
    val missingMode: String? = missingModeInput.logDeliveryMode
    assertNull(missingMode)

    listOf(
      missingModeInput,
      sidecarInput(""),
      sidecarInput(" "),
      sidecarInput("not-a-mode"),
      sidecarInput("FUTURE"),
    ).forEach { input ->
      val harness = harness(sidecarInput = input)

      assertDoesNotThrow { harness.initializer.initialize() }

      assertSame(harness.standard, harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
      assertSame(harness.console, harness.root.getAppender(CONSOLE_APPENDER_NAME))
      assertSame(harness.audit, harness.root.getAppender(AUDIT_LOGGER_NAME))
      assertSame(harness.replicationDump, harness.root.getAppender(CLOUD_REPLICATION_JOB_DUMPER_NAME))
      assertFalse(harness.standard.stopped)
      assertNull(harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
      verify(exactly = 0) { harness.workloadApiClient.workloadGet(any()) }
      verify(exactly = 0) { harness.workloadApiClient.workloadLogUploadAuthorization(any()) }
      harness.context.stop()
    }
  }

  @ParameterizedTest
  @EnumSource(value = SidecarInput.OperationType::class, names = ["CHECK", "DISCOVER"])
  fun `flex input installs flex without workload lookup`(operationType: SidecarInput.OperationType) {
    val flex = TrackingAppender()
    val harness =
      harness(
        operationType = operationType,
        mode = LogDeliveryMode.FLEX.name,
        appenderFactory = { _, _, _, _ -> flex },
      )
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns initialAuthorization()

    harness.initializer.initialize()

    assertSame(flex, harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
    assertSame(harness.console, harness.root.getAppender(CONSOLE_APPENDER_NAME))
    assertSame(harness.audit, harness.root.getAppender(AUDIT_LOGGER_NAME))
    assertSame(harness.replicationDump, harness.root.getAppender(CLOUD_REPLICATION_JOB_DUMPER_NAME))
    verify(exactly = 0) { harness.workloadApiClient.workloadGet(any()) }
    verify(exactly = 1) { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) }
    harness.context.stop()
  }

  @ParameterizedTest
  @EnumSource(value = SidecarInput.OperationType::class, names = ["CHECK", "DISCOVER"])
  fun `standard input leaves standard topology without workload lookup`(operationType: SidecarInput.OperationType) {
    val harness = harness(operationType = operationType, mode = LogDeliveryMode.STANDARD.name)

    harness.initializer.initialize()

    assertSame(harness.standard, harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
    assertNull(harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
    verify(exactly = 0) { harness.workloadApiClient.workloadGet(any()) }
    verify(exactly = 0) { harness.workloadApiClient.workloadLogUploadAuthorization(any()) }
    harness.context.stop()
  }

  @Test
  fun `spec does not call mode or authorization APIs and leaves appenders untouched`() {
    val harness = harness(operationType = SidecarInput.OperationType.SPEC)

    harness.initializer.initialize()

    assertSame(harness.standard, harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
    assertFalse(harness.standard.stopped)
    assertNull(harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
    verify(exactly = 0) { harness.workloadApiClient.workloadGet(any()) }
    verify(exactly = 0) { harness.workloadApiClient.workloadLogUploadAuthorization(any()) }
    harness.context.stop()
  }

  @ParameterizedTest
  @EnumSource(value = SidecarInput.OperationType::class, names = ["CHECK", "DISCOVER"])
  fun `valid flex installs one started flex appender before replacing standard`(operationType: SidecarInput.OperationType) {
    val actions = mutableListOf<String>()
    lateinit var flex: TrackingAppender
    lateinit var initialTarget: GcsLogUploadTarget
    lateinit var refresh: () -> LogUploadAuthorizationRefreshResult
    val harness =
      harness(
        operationType = operationType,
        mode = LogDeliveryMode.FLEX.name,
        standard = TrackingAppender(onStop = { actions += "standard-stop" }),
        appenderFactory = { target, callback, _, onStop ->
          actions += "factory"
          initialTarget = target
          refresh = callback
          TrackingAppender(onStart = { actions += "flex-start" }, onStop = onStop).also { flex = it }
        },
      )
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } answers {
      assertSame(harness.standard, harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
      actions += "authorization"
      initialAuthorization()
    }

    harness.initializer.initialize()

    assertEquals(listOf("authorization", "factory", "flex-start"), actions)
    assertEquals(INITIAL_TARGET, initialTarget)
    assertNull(harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
    assertSame(flex, harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
    assertSame(harness.console, harness.root.getAppender(CONSOLE_APPENDER_NAME))
    assertSame(harness.audit, harness.root.getAppender(AUDIT_LOGGER_NAME))
    assertSame(harness.replicationDump, harness.root.getAppender(CLOUD_REPLICATION_JOB_DUMPER_NAME))
    assertTrue(harness.standard.isStarted)
    assertFalse(harness.standard.stopped)
    assertTrue(flex.isStarted)
    verify(exactly = 0) { harness.workloadApiClient.workloadGet(any()) }
    verify(exactly = 1) { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) }

    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns refreshedAuthorization()
    assertEquals(LogUploadAuthorizationRefreshResult.Refreshed(REFRESHED_TARGET), refresh())
    verify(exactly = 2) { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) }

    harness.context.stop()
    assertTrue(flex.stopped)
    assertTrue(harness.standard.stopped)
  }

  @Test
  fun `interrupted runtime refresh detaches flex and restores usable standard delivery`() {
    val diagnostics = mutableListOf<String>()
    val writes = AtomicInteger()
    lateinit var credentials: OAuth2CredentialsWithRefresh
    lateinit var uploader: RefreshableGcsLogUploader<String>
    val harness =
      harness(
        mode = LogDeliveryMode.FLEX.name,
        appenderFactory = { target, refresh, onFailure, _ ->
          uploader =
            testUploader(
              initialTarget = target,
              refreshAuthorization = refresh,
              encode = { it.single() },
              onFailure = onFailure,
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
                    writes.incrementAndGet()
                    credentials.refresh()
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
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns initialAuthorization()
    harness.initializer.initialize()
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } throws
      InterruptedException("sensitive refresh")

    try {
      runBlocking { uploader.append("first") }
      assertDoesNotThrow { uploader.flush() }
      runBlocking { uploader.append("ignored") }
      uploader.flush()
      val event = LoggingEvent()
      harness.root.callAppenders(event)

      assertTrue(Thread.currentThread().isInterrupted)
      assertEquals(1, writes.get())
      assertTrue(uploader.isDisabled)
      assertSame(harness.standard, harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
      assertTrue(harness.standard.isStarted)
      assertEquals(listOf(event), harness.standard.events)
      assertNull(harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
      assertEquals(listOf("$FLEX_INITIALIZATION_FAILURE_MESSAGE Failure stage: FLEX runtime delivery."), diagnostics)
      assertTrue(diagnostics.none { it.contains("sensitive") })
      verify(exactly = 2) { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) }
    } finally {
      Thread.interrupted()
      harness.context.stop()
    }
  }

  @Test
  fun `job MDC event follows server prefix topology through flex and not standard`() {
    val flex = TrackingAppender()
    lateinit var target: GcsLogUploadTarget
    val harness =
      harness(
        mode = LogDeliveryMode.FLEX.name,
        appenderFactory = { initialTarget, _, _, _ ->
          target = initialTarget
          flex
        },
      )
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns initialAuthorization()
    harness.initializer.initialize()
    val event = LoggingEvent().apply { mdcPropertyMap = mapOf(DEFAULT_JOB_LOG_PATH_MDC_KEY to "server-issued/job/attempt") }

    harness.root.callAppenders(event)

    assertEquals(INITIAL_PREFIX, target.objectKeyPrefix)
    assertEquals(listOf(event), flex.events)
    assertTrue(harness.standard.events.isEmpty())
    harness.context.stop()
  }

  @Test
  fun `null or invalid flex authorization preserves standard remote delivery`() {
    val authorizations =
      listOf<LogUploadAuthorization?>(
        null,
        GcsDownscopedOAuthLogUploadAuthorization("", EXPIRES_AT, "bucket", INITIAL_PREFIX),
        GcsDownscopedOAuthLogUploadAuthorization("token", EXPIRES_AT, "", INITIAL_PREFIX),
        GcsDownscopedOAuthLogUploadAuthorization("token", EXPIRES_AT, "bucket", "invalid-prefix"),
      )

    authorizations.forEach { authorization ->
      val diagnostics = mutableListOf<String>()
      val harness =
        harness(
          mode = LogDeliveryMode.FLEX.name,
          diagnostics = diagnostics::add,
        )
      every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns authorization

      assertDoesNotThrow { harness.initializer.initialize() }

      assertSame(harness.standard, harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
      assertNull(harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
      assertSame(harness.console, harness.root.getAppender(CONSOLE_APPENDER_NAME))
      assertSame(harness.audit, harness.root.getAppender(AUDIT_LOGGER_NAME))
      assertSame(harness.replicationDump, harness.root.getAppender(CLOUD_REPLICATION_JOB_DUMPER_NAME))
      assertFalse(harness.standard.stopped)
      assertSanitized(diagnostics, "initial FLEX authorization")
      verify(exactly = 0) { harness.workloadApiClient.workloadGet(any()) }
      harness.context.stop()
    }
  }

  @Test
  fun `authorization failures after flex is known preserve standard delivery`() {
    listOf(
      IOException("sensitive token"),
      ApiException(500, "sensitive-url", "sensitive-body"),
      ApiException(410, "sensitive-url", "sensitive-body"),
      object : JsonProcessingException("sensitive malformed authorization") {},
      IllegalStateException("sensitive prefix"),
      AssertionError("sensitive authorization token"),
    ).forEach { failure ->
      val diagnostics = mutableListOf<String>()
      val harness =
        harness(
          mode = LogDeliveryMode.FLEX.name,
          diagnostics = diagnostics::add,
        )
      every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } throws failure

      assertDoesNotThrow { harness.initializer.initialize() }

      assertSame(harness.standard, harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
      assertNull(harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
      assertFalse(harness.standard.stopped)
      assertSame(harness.console, harness.root.getAppender(CONSOLE_APPENDER_NAME))
      assertSanitized(diagnostics, "initial FLEX authorization")
      verify(exactly = 0) { harness.workloadApiClient.workloadGet(any()) }
      harness.context.stop()
    }
  }

  @Test
  fun `construction or startup failure after flex is known preserves standard and is nonfatal`() {
    val failures =
      listOf<(GcsLogUploadTarget, () -> LogUploadAuthorizationRefreshResult, (String) -> Unit, () -> Unit) -> Appender<ILoggingEvent>>(
        { _, _, _, _ -> throw IOException("sensitive construction token") },
        { _, _, _, _ -> throw AssertionError("sensitive construction token") },
        { _, _, _, _ -> TrackingAppender(startFailure = IllegalStateException("sensitive startup prefix")) },
        { _, _, _, _ -> TrackingAppender(startSuccessfully = false) },
      )

    failures.forEach { factory ->
      val diagnostics = mutableListOf<String>()
      val harness =
        harness(
          mode = LogDeliveryMode.FLEX.name,
          appenderFactory = factory,
          diagnostics = diagnostics::add,
        )
      every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns initialAuthorization()

      assertDoesNotThrow { harness.initializer.initialize() }

      assertSame(harness.standard, harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
      assertNull(harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
      assertSame(harness.console, harness.root.getAppender(CONSOLE_APPENDER_NAME))
      assertFalse(harness.standard.stopped)
      assertTrue(harness.standard.isStarted)
      val event = LoggingEvent()
      harness.root.callAppenders(event)
      assertEquals(listOf(event), harness.standard.events)
      assertSanitized(diagnostics, "FLEX appender installation")
      harness.context.stop()
    }
  }

  @Test
  fun `periodic scheduler rejection from real flex appender preserves standard and reports installation failure`() {
    val diagnostics = mutableListOf<String>()
    val harness =
      harness(
        mode = LogDeliveryMode.FLEX.name,
        appenderFactory = { target, refresh, onFailure, onStop -> AirbyteFlexLogbackAppender(target, refresh, onFailure, onStop) },
        diagnostics = diagnostics::add,
      )
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns initialAuthorization()
    mockkObject(CloudStorageBulkUploaderExecutor)
    try {
      every { CloudStorageBulkUploaderExecutor.scheduleTask(any(), any(), any(), any()) } throws
        RejectedExecutionException("sensitive token at $INITIAL_PREFIX")

      assertDoesNotThrow { harness.initializer.initialize() }

      assertSame(harness.standard, harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
      assertTrue(harness.standard.isStarted)
      val event = LoggingEvent()
      harness.root.callAppenders(event)
      assertEquals(listOf(event), harness.standard.events)
      assertNull(harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
      assertSanitized(diagnostics, "FLEX appender installation")
    } finally {
      harness.context.stop()
      unmockkObject(CloudStorageBulkUploaderExecutor)
    }
  }

  @Test
  fun `diagnostic assertion error is suppressed while standard remains attached`() {
    val harness =
      harness(
        mode = LogDeliveryMode.FLEX.name,
        diagnostics = { throw AssertionError("sensitive diagnostic") },
      )
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns null

    assertDoesNotThrow { harness.initializer.initialize() }

    assertSame(harness.standard, harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
    assertTrue(harness.standard.isStarted)
    harness.context.stop()
  }

  @Test
  fun `diagnostic interruption is suppressed and restores interrupt status`() {
    val harness =
      harness(
        mode = LogDeliveryMode.FLEX.name,
        diagnostics = { throw InterruptedException("sensitive diagnostic") },
      )
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
  fun `virtual machine error from diagnostics propagates while standard remains attached`() {
    val failure = OutOfMemoryError("sensitive diagnostic")
    val harness =
      harness(
        mode = LogDeliveryMode.FLEX.name,
        diagnostics = { throw failure },
      )
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns null

    assertSame(failure, assertThrows(OutOfMemoryError::class.java) { harness.initializer.initialize() })
    assertSame(harness.standard, harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
    harness.context.stop()
  }

  @Test
  fun `interrupted authorization and installation failures restore interrupt status and preserve standard`() {
    val authorizationHarness = harness(mode = LogDeliveryMode.FLEX.name)
    every { authorizationHarness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } throws
      InterruptedException("sensitive authorization")
    assertInterruptedFallback(authorizationHarness)

    val installationHarness =
      harness(
        mode = LogDeliveryMode.FLEX.name,
        appenderFactory = { _, _, _, _ -> throw InterruptedException("sensitive installation") },
      )
    every { installationHarness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns initialAuthorization()
    assertInterruptedFallback(installationHarness)
  }

  @Test
  fun `interrupted restoration never attaches a stopped standard appender`() {
    val standard = TrackingAppender(restartFailure = InterruptedException("sensitive restoration"))
    val harness =
      harness(
        mode = LogDeliveryMode.FLEX.name,
        standard = standard,
        appenderFactory = { _, _, _, _ -> throw IOException("sensitive installation") },
      )
    standard.stop()
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns initialAuthorization()

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
  fun `virtual machine errors from authorization and installation propagate`() {
    val authorizationFailure = OutOfMemoryError("sensitive authorization")
    val authorizationHarness = harness(mode = LogDeliveryMode.FLEX.name)
    every { authorizationHarness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } throws authorizationFailure
    assertSame(authorizationFailure, assertThrows(OutOfMemoryError::class.java) { authorizationHarness.initializer.initialize() })
    assertSame(authorizationHarness.standard, authorizationHarness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
    authorizationHarness.context.stop()

    val installationFailure = OutOfMemoryError("sensitive installation")
    val installationHarness =
      harness(
        mode = LogDeliveryMode.FLEX.name,
        appenderFactory = { _, _, _, _ -> throw installationFailure },
      )
    every { installationHarness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns initialAuthorization()
    assertSame(installationFailure, assertThrows(OutOfMemoryError::class.java) { installationHarness.initializer.initialize() })
    assertSame(installationHarness.standard, installationHarness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
    installationHarness.context.stop()
  }

  @Test
  fun `standard shutdown failure remains nonfatal during normal flex context shutdown`() {
    listOf<Throwable>(
      IllegalStateException("sensitive stop failure"),
      AssertionError("sensitive stop failure"),
    ).forEach { failure ->
      val diagnostics = mutableListOf<String>()
      val standard = TrackingAppender(stopFailure = failure)
      lateinit var flex: TrackingAppender
      val harness =
        harness(
          mode = LogDeliveryMode.FLEX.name,
          standard = standard,
          appenderFactory = { _, _, _, onStop -> TrackingAppender(onStop = onStop).also { flex = it } },
          diagnostics = diagnostics::add,
        )
      every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns initialAuthorization()

      assertDoesNotThrow { harness.initializer.initialize() }

      assertTrue(standard.isStarted)
      assertNull(harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
      assertSame(flex, harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
      assertDoesNotThrow { harness.context.stop() }
      assertTrue(standard.stopped)
      assertSanitized(diagnostics, "FLEX runtime delivery")
    }
  }

  @Test
  fun `cleanup failure while recovering remains nonfatal and restores standard`() {
    val diagnostics = mutableListOf<String>()
    val standard = TrackingAppender()
    val flex =
      TrackingAppender(
        startFailure = IOException("sensitive flex startup failure"),
        stopFailure = AssertionError("sensitive flex cleanup failure"),
        failAfterStart = true,
      )
    val harness =
      harness(
        mode = LogDeliveryMode.FLEX.name,
        standard = standard,
        appenderFactory = { _, _, _, _ -> flex },
        diagnostics = diagnostics::add,
      )
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns initialAuthorization()

    assertDoesNotThrow { harness.initializer.initialize() }

    assertSame(standard, harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
    assertTrue(standard.isStarted)
    assertNull(harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
    assertTrue(flex.stopped)
    assertSanitized(diagnostics, "FLEX appender installation")
    harness.context.stop()
  }

  @Test
  fun `refresh adapter retries only IO and 5xx and treats every other result as terminal`() {
    lateinit var refresh: () -> LogUploadAuthorizationRefreshResult
    val harness =
      harness(
        mode = LogDeliveryMode.FLEX.name,
        appenderFactory = { _, callback, _, _ ->
          refresh = callback
          TrackingAppender()
        },
      )
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns initialAuthorization()
    harness.initializer.initialize()

    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns null
    assertSame(LogUploadAuthorizationRefreshResult.TerminalFailure, refresh())
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns
      GcsDownscopedOAuthLogUploadAuthorization("", EXPIRES_AT, "bucket", INITIAL_PREFIX)
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
    harness.context.stop()
  }

  private fun harness(
    operationType: SidecarInput.OperationType = SidecarInput.OperationType.CHECK,
    mode: String? = null,
    sidecarInput: SidecarInput? = null,
    standard: TrackingAppender = TrackingAppender(),
    appenderFactory: (
      GcsLogUploadTarget,
      () -> LogUploadAuthorizationRefreshResult,
      (String) -> Unit,
      () -> Unit,
    ) -> Appender<ILoggingEvent> = { _, _, _, _ -> TrackingAppender() },
    diagnostics: (String) -> Unit = {},
  ): Harness {
    val context = LoggerContext()
    val root = context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
    standard.name = CLOUD_OPERATIONS_JOB_LOGGER_NAME
    standard.start()
    val console = TrackingAppender().apply { name = CONSOLE_APPENDER_NAME }
    val audit = TrackingAppender().apply { name = AUDIT_LOGGER_NAME }
    val replicationDump = TrackingAppender().apply { name = CLOUD_REPLICATION_JOB_DUMPER_NAME }
    listOf(console, audit, replicationDump).forEach {
      it.start()
      root.addAppender(it)
    }
    root.addAppender(standard)
    val input =
      sidecarInput ?: mockk<SidecarInput>().also {
        every { it.operationType } returns operationType
        every { it.workloadId } returns WORKLOAD_ID
        every { it.logDeliveryMode } returns mode
      }
    val workloadApiClient = mockk<WorkloadApiClient>()
    val initializer =
      FlexLogAppenderInitializer(
        sidecarInput = input,
        workloadApiClient = workloadApiClient,
        loggerContextProvider = { context },
        appenderFactory = appenderFactory,
        diagnostics = diagnostics,
      )
    return Harness(context, root, standard, console, audit, replicationDump, workloadApiClient, initializer)
  }

  @Suppress("UNCHECKED_CAST")
  private fun <T : Any> testUploader(
    initialTarget: GcsLogUploadTarget,
    refreshAuthorization: () -> LogUploadAuthorizationRefreshResult,
    encode: (List<T>) -> String,
    onFailure: (String) -> Unit,
    storageClientFactory: (String, OAuth2CredentialsWithRefresh) -> StorageClient,
  ): RefreshableGcsLogUploader<T> {
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

  private fun sidecarInput(mode: String?) =
    SidecarInput(
      checkConnectionInput = null,
      discoverCatalogInput = null,
      workloadId = WORKLOAD_ID,
      integrationLauncherConfig = IntegrationLauncherConfig(),
      operationType = SidecarInput.OperationType.CHECK,
      logPath = INITIAL_PREFIX,
      logDeliveryMode = mode,
    )

  private fun initialAuthorization(): GcsDownscopedOAuthLogUploadAuthorization =
    GcsDownscopedOAuthLogUploadAuthorization("initial-token", EXPIRES_AT, "initial-bucket", INITIAL_PREFIX)

  private fun refreshedAuthorization(): GcsDownscopedOAuthLogUploadAuthorization =
    GcsDownscopedOAuthLogUploadAuthorization("refreshed-token", REFRESHED_EXPIRES_AT, "initial-bucket", INITIAL_PREFIX)

  private fun assertSanitized(
    diagnostics: List<String>,
    expectedStage: String,
  ) {
    assertEquals(listOf("$FLEX_INITIALIZATION_FAILURE_MESSAGE Failure stage: $expectedStage."), diagnostics)
    assertTrue(diagnostics.none { message -> SENSITIVE_VALUES.any(message::contains) })
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

  private data class Harness(
    val context: LoggerContext,
    val root: ch.qos.logback.classic.Logger,
    val standard: TrackingAppender,
    val console: TrackingAppender,
    val audit: TrackingAppender,
    val replicationDump: TrackingAppender,
    val workloadApiClient: WorkloadApiClient,
    val initializer: FlexLogAppenderInitializer,
  )

  private class TrackingAppender(
    private val onStart: () -> Unit = {},
    private val onStop: () -> Unit = {},
    private val startFailure: Throwable? = null,
    private var stopFailure: Throwable? = null,
    private var restartFailure: Throwable? = null,
    private val startSuccessfully: Boolean = true,
    private val failAfterStart: Boolean = false,
  ) : AppenderBase<ILoggingEvent>() {
    val events = mutableListOf<ILoggingEvent>()
    var stopped = false
    private var startCount = 0

    override fun start() {
      startCount += 1
      onStart()
      if (startCount == 1) {
        startFailure?.let {
          if (failAfterStart) {
            stopped = false
            super.start()
          }
          throw it
        }
      } else {
        restartFailure?.let {
          restartFailure = null
          throw it
        }
      }
      if (startSuccessfully) {
        stopped = false
        super.start()
      }
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

    override fun append(eventObject: ILoggingEvent) {
      events += eventObject
    }
  }

  private companion object {
    const val WORKLOAD_ID = "workload-id"
    const val CONSOLE_APPENDER_NAME = "console"
    const val INITIAL_PREFIX = "job-logging/workload/job/attempt/"
    val EXPIRES_AT: OffsetDateTime = OffsetDateTime.parse("2030-01-01T00:00:00Z")
    val REFRESHED_EXPIRES_AT: OffsetDateTime = OffsetDateTime.parse("2030-01-01T01:00:00Z")
    val INITIAL_TARGET = GcsLogUploadTarget("initial-token", EXPIRES_AT, "initial-bucket", INITIAL_PREFIX)
    val REFRESHED_TARGET = GcsLogUploadTarget("refreshed-token", REFRESHED_EXPIRES_AT, "initial-bucket", INITIAL_PREFIX)
    val SENSITIVE_VALUES = listOf("sensitive", "initial-token", "refreshed-token", "initial-bucket", INITIAL_PREFIX)
  }
}
