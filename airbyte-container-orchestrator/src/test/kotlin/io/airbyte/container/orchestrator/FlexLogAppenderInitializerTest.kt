/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import ch.qos.logback.core.AppenderBase
import com.fasterxml.jackson.core.JsonProcessingException
import com.google.auth.oauth2.OAuth2CredentialsWithRefresh
import io.airbyte.api.client.ApiException
import io.airbyte.commons.logging.logback.CLOUD_OPERATIONS_JOB_LOGGER_NAME
import io.airbyte.commons.logging.logback.FLEX_OPERATIONS_JOB_LOGGER_NAME
import io.airbyte.commons.storage.CloudStorageBulkUploaderExecutor
import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.GcsLogUploadTarget
import io.airbyte.commons.storage.LogUploadAuthorizationRefreshResult
import io.airbyte.commons.storage.RefreshableGcsLogUploader
import io.airbyte.commons.storage.StorageClient
import io.airbyte.micronaut.runtime.AirbyteConnectorConfig
import io.airbyte.micronaut.runtime.AirbyteContextConfig
import io.airbyte.micronaut.runtime.StorageType
import io.airbyte.workload.api.client.WorkloadApiClient
import io.mockk.every
import io.mockk.mockk
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
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

internal class FlexLogAppenderInitializerTest {
  @Test
  fun `missing malformed and unsupported modes leave every appender untouched`(
    @TempDir configDir: Path,
  ) {
    listOf<String?>(null, "", "not-json", "\"FUTURE\"").forEach { mode ->
      val harness = harness(configDir)
      mode?.let { Files.writeString(configDir.resolve(MODE_FILE), it) }

      assertDoesNotThrow { harness.initializer.initialize() }

      assertSame(harness.standard, harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
      assertSame(harness.unrelated, harness.root.getAppender("unrelated"))
      assertFalse(harness.standard.stopped)
      harness.context.stop()
      Files.deleteIfExists(configDir.resolve(MODE_FILE))
    }
  }

  @Test
  fun `explicit standard ignores an authorization file and leaves every appender untouched`(
    @TempDir configDir: Path,
  ) {
    Files.writeString(configDir.resolve(MODE_FILE), "\"STANDARD\"")
    Files.writeString(configDir.resolve(AUTH_FILE), validAuthorizationJson())
    val harness = harness(configDir)

    harness.initializer.initialize()

    assertSame(harness.standard, harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
    assertSame(harness.unrelated, harness.root.getAppender("unrelated"))
    assertNull(harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
    verify(exactly = 0) { harness.workloadApiClient.workloadLogUploadAuthorization(any()) }
  }

  @Test
  fun `flex without a valid initial authorization disables only standard remote delivery`(
    @TempDir configDir: Path,
  ) {
    listOf<String?>(null, "", "null", "{}", "{\"type\":\"FUTURE\"}").forEach { authorization ->
      val harness = harness(configDir)
      Files.writeString(configDir.resolve(MODE_FILE), "\"FLEX\"")
      authorization?.let { Files.writeString(configDir.resolve(AUTH_FILE), it) }

      assertDoesNotThrow { harness.initializer.initialize() }

      assertNull(harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
      assertNull(harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
      assertTrue(harness.standard.stopped)
      assertSame(harness.unrelated, harness.root.getAppender("unrelated"))
      harness.context.stop()
      Files.deleteIfExists(configDir.resolve(AUTH_FILE))
    }
  }

  @Test
  fun `valid flex replaces only standard operations appender`(
    @TempDir configDir: Path,
  ) {
    Files.writeString(configDir.resolve(MODE_FILE), "\"FLEX\"")
    Files.writeString(configDir.resolve(AUTH_FILE), validAuthorizationJson())
    val flex = TrackingAppender()
    val harness = harness(configDir, appenderFactory = { _, _ -> flex })

    harness.initializer.initialize()

    assertNull(harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
    assertSame(flex, harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
    assertSame(harness.unrelated, harness.root.getAppender("unrelated"))
    assertTrue(harness.standard.stopped)
    assertTrue(flex.isStarted)
  }

  @Test
  fun `failure building flex appender remains non-fatal without restoring standard delivery`(
    @TempDir configDir: Path,
  ) {
    Files.writeString(configDir.resolve(MODE_FILE), "\"FLEX\"")
    Files.writeString(configDir.resolve(AUTH_FILE), validAuthorizationJson())
    val harness = harness(configDir, appenderFactory = { _, _ -> throw IOException("sensitive token and prefix") })

    assertDoesNotThrow { harness.initializer.initialize() }

    assertNull(harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
    assertNull(harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
    assertSame(harness.unrelated, harness.root.getAppender("unrelated"))
  }

  @Test
  fun `flex authorization with an invalid object prefix does not install an appender`(
    @TempDir configDir: Path,
  ) {
    Files.writeString(configDir.resolve(MODE_FILE), "\"FLEX\"")
    Files.writeString(configDir.resolve(AUTH_FILE), validAuthorizationJson().replace("job-logging/workload/", "other/workload"))
    val harness = harness(configDir)

    harness.initializer.initialize()

    assertNull(harness.root.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME))
    assertNull(harness.root.getAppender(FLEX_OPERATIONS_JOB_LOGGER_NAME))
  }

  @Test
  fun `refresh adapter retries only IO and 5xx and treats every other outcome as terminal`(
    @TempDir configDir: Path,
  ) {
    Files.writeString(configDir.resolve(MODE_FILE), "\"FLEX\"")
    Files.writeString(configDir.resolve(AUTH_FILE), validAuthorizationJson())
    lateinit var refresh: () -> LogUploadAuthorizationRefreshResult
    val harness =
      harness(configDir, appenderFactory = { _, callback ->
        refresh = callback
        TrackingAppender()
      })
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } returns null
    harness.initializer.initialize()

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
    Files.writeString(configDir.resolve(MODE_FILE), "\"FLEX\"")
    Files.writeString(configDir.resolve(AUTH_FILE), validAuthorizationJson())
    lateinit var initialTarget: GcsLogUploadTarget
    lateinit var refresh: () -> LogUploadAuthorizationRefreshResult
    val harness =
      harness(configDir, appenderFactory = { target, callback ->
        initialTarget = target
        refresh = callback
        TrackingAppender()
      })
    var uploaderForRefresh: RefreshableGcsLogUploader<String>? = null
    lateinit var credentials: OAuth2CredentialsWithRefresh
    val gcsCalls = AtomicInteger()
    val diagnostics = mutableListOf<String>()
    every { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) } answers {
      runBlocking { uploaderForRefresh!!.append("queued-during-refresh") }
      throw ApiException(200, "sensitive-url", "sensitive-body")
    }
    harness.initializer.initialize()
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
    verify(exactly = 1) { harness.workloadApiClient.workloadLogUploadAuthorization(WORKLOAD_ID) }
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
    appenderFactory: (GcsLogUploadTarget, () -> LogUploadAuthorizationRefreshResult) -> Appender<ILoggingEvent> = { _, _ -> TrackingAppender() },
  ): Harness {
    val context = LoggerContext()
    val root = context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
    val standard =
      TrackingAppender().apply {
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
    val initializer =
      FlexLogAppenderInitializer(
        connectorConfig = AirbyteConnectorConfig(configDir = configDir.toString()),
        contextConfig = AirbyteContextConfig(workloadId = WORKLOAD_ID),
        workloadApiClient = workloadApiClient,
        loggerContextProvider = { context },
        appenderFactory = appenderFactory,
      )
    return Harness(context, root, standard, unrelated, workloadApiClient, initializer)
  }

  private fun validAuthorizationJson(): String =
    """{"type":"GCS_DOWNSCOPED_OAUTH","accessToken":"token","expiresAt":"2030-01-01T00:00:00Z","bucketName":"bucket","objectKeyPrefix":"job-logging/workload/"}"""

  private data class Harness(
    val context: LoggerContext,
    val root: ch.qos.logback.classic.Logger,
    val standard: TrackingAppender,
    val unrelated: TrackingAppender,
    val workloadApiClient: WorkloadApiClient,
    val initializer: FlexLogAppenderInitializer,
  )

  private class TrackingAppender : AppenderBase<ILoggingEvent>() {
    var stopped = false

    override fun append(eventObject: ILoggingEvent) = Unit

    override fun stop() {
      stopped = true
      super.stop()
    }
  }

  private companion object {
    const val MODE_FILE = "log-delivery-mode.json"
    const val AUTH_FILE = "log-upload-authorization.json"
    const val WORKLOAD_ID = "workload-id"
  }
}
