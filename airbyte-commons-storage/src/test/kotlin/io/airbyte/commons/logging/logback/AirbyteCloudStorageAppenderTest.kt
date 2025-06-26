/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging.logback

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.ThrowableProxy
import ch.qos.logback.core.Context
import ch.qos.logback.core.status.ErrorStatus
import ch.qos.logback.core.status.Status
import ch.qos.logback.core.status.StatusManager
import io.airbyte.commons.envvar.EnvVar
import io.airbyte.commons.resources.MoreResources
import io.airbyte.commons.storage.AzureStorageClient
import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.GcsStorageClient
import io.airbyte.commons.storage.LocalStorageClient
import io.airbyte.commons.storage.MinioStorageClient
import io.airbyte.commons.storage.S3StorageClient
import io.airbyte.commons.storage.STRUCTURED_LOG_FILE_EXTENSION
import io.airbyte.commons.storage.StorageClient
import io.airbyte.commons.storage.createFileId
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.io.IOException
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.io.path.Path

private class AirbyteCloudStorageAppenderTest {
  @AfterEach
  fun tearDown() {
    Files.newDirectoryStream(Path("."), "*.log").use { stream ->
      stream.forEach { Files.deleteIfExists(it) }
    }
  }

  @Test
  fun testBuildBucketConfig() {
    val bucket = "test-bucket"
    val storageConfig =
      mapOf(
        EnvVar.STORAGE_BUCKET_LOG to bucket,
      )
    val bucketConfig = buildBucketConfig(storageConfig)
    assertEquals(bucket, bucketConfig.log)
    assertEquals("", bucketConfig.state)
    assertEquals("", bucketConfig.workloadOutput)
    assertEquals("", bucketConfig.activityPayload)
    assertEquals("", bucketConfig.auditLogging)
  }

  @Test
  fun testBuildAzureStorageClient() {
    val bucket = "test-bucket"
    val connectionString = "AccountName=test;AccountKey=test-key"
    val storageType = "azure"
    val storageConfig =
      mapOf(
        EnvVar.STORAGE_TYPE to storageType,
        EnvVar.STORAGE_BUCKET_LOG to bucket,
        EnvVar.AZURE_STORAGE_CONNECTION_STRING to connectionString,
      )
    val client = buildStorageClient(storageConfig = storageConfig, documentType = DocumentType.LOGS)
    assertEquals(AzureStorageClient::class.java, client.javaClass)
  }

  @Test
  fun testBuildGcsStorageClient() {
    val bucket = "test-bucket"
    val applicationCredentials = MoreResources.readResourceAsFile("sample_gcs_credentials.json")
    val credentials = applicationCredentials.path
    val storageType = "gcs"
    val storageConfig =
      mapOf(
        EnvVar.STORAGE_TYPE to storageType,
        EnvVar.STORAGE_BUCKET_LOG to bucket,
        EnvVar.GOOGLE_APPLICATION_CREDENTIALS to credentials,
      )
    val client = buildStorageClient(storageConfig = storageConfig, documentType = DocumentType.LOGS)
    assertEquals(GcsStorageClient::class.java, client.javaClass)
  }

  @Test
  fun testBuildMinioStorageClient() {
    val bucket = "test-bucket"
    val accessKey = "test_access_key"
    val accessSecretKey = "test_access_secret_key"
    val endpoint = "test-endpoint:9000"
    val storageType = "minio"
    val storageConfig =
      mapOf(
        EnvVar.STORAGE_TYPE to storageType,
        EnvVar.STORAGE_BUCKET_LOG to bucket,
        EnvVar.AWS_ACCESS_KEY_ID to accessKey,
        EnvVar.AWS_SECRET_ACCESS_KEY to accessSecretKey,
        EnvVar.MINIO_ENDPOINT to endpoint,
      )
    val client = buildStorageClient(storageConfig = storageConfig, documentType = DocumentType.LOGS)
    assertEquals(MinioStorageClient::class.java, client.javaClass)
  }

  @Test
  fun testBuildS3StorageClient() {
    val bucket = "test-bucket"
    val accessKey = "test_access_key"
    val accessSecretKey = "test_access_secret_key"
    val region = "US-EAST-1"
    val storageType = "s3"
    val storageConfig =
      mapOf(
        EnvVar.STORAGE_TYPE to storageType,
        EnvVar.STORAGE_BUCKET_LOG to bucket,
        EnvVar.AWS_ACCESS_KEY_ID to accessKey,
        EnvVar.AWS_SECRET_ACCESS_KEY to accessSecretKey,
        EnvVar.AWS_DEFAULT_REGION to region,
      )
    val client = buildStorageClient(storageConfig = storageConfig, documentType = DocumentType.LOGS)
    assertEquals(S3StorageClient::class.java, client.javaClass)
  }

  @Test
  fun testBuildLocalStorageClient() {
    val bucket = "test-bucket"
    val storageType = "local"
    val storageConfig =
      mapOf(
        EnvVar.STORAGE_TYPE to storageType,
        EnvVar.STORAGE_BUCKET_LOG to bucket,
      )
    val client = buildStorageClient(storageConfig = storageConfig, documentType = DocumentType.LOGS)
    assertEquals(LocalStorageClient::class.java, client.javaClass)
  }

  @Test
  fun testBuildDefaultStorageClient() {
    val bucket = "test-bucket"
    val storageType = "unknown"
    val storageConfig =
      mapOf(
        EnvVar.STORAGE_TYPE to storageType,
        EnvVar.STORAGE_BUCKET_LOG to bucket,
      )
    val client = buildStorageClient(storageConfig = storageConfig, documentType = DocumentType.LOGS)
    assertEquals(LocalStorageClient::class.java, client.javaClass)

    val storageConfig2 =
      mapOf(
        EnvVar.STORAGE_BUCKET_LOG to bucket,
      )
    val client2 = buildStorageClient(storageConfig = storageConfig2, documentType = DocumentType.LOGS)
    assertEquals(LocalStorageClient::class.java, client2.javaClass)
  }

  @Test
  fun testStorageUpload() {
    val baseStorageId = "/path/to/logs"
    val storageClient = mockk<StorageClient>()
    val className = "io.airbyte.TestClass"
    val context = emptyMap<String, String>()
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
    val period = 1L
    val statusManager =
      mockk<StatusManager> {
        every { add(any<Status>()) } returns Unit
      }
    val loggingContext =
      mockk<Context> {
        every { getStatusManager() } returns statusManager
      }

    val appender =
      AirbyteCloudStorageAppender(
        documentType = DocumentType.LOGS,
        storageClient = storageClient,
        baseStorageId = baseStorageId,
        period = period,
        unit = TimeUnit.SECONDS,
      )
    appender.context = loggingContext
    appender.start()

    appender.doAppend(event)

    Thread.sleep(TimeUnit.SECONDS.toMillis(period * 2))

    verify(exactly = 1) { storageClient.write(any<String>(), any<String>()) }
  }

  @Test
  fun testStorageUploadException() {
    val baseStorageId = "/path/to/logs"
    val storageClient =
      mockk<StorageClient> {
        every { write(any<String>(), any<String>()) } throws IOException("test")
      }
    val className = "io.airbyte.TestClass"
    val context = emptyMap<String, String>()
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
    val period = 1L
    val statusManager =
      mockk<StatusManager> {
        every { add(any<Status>()) } returns Unit
      }
    val loggingContext =
      mockk<Context> {
        every { getStatusManager() } returns statusManager
      }

    val appender =
      AirbyteCloudStorageAppender(
        documentType = DocumentType.LOGS,
        storageClient = storageClient,
        baseStorageId = baseStorageId,
        period = period,
        unit = TimeUnit.SECONDS,
      )
    appender.context = loggingContext
    appender.start()

    appender.doAppend(event)

    Thread.sleep(TimeUnit.SECONDS.toMillis(period * 2))

    verify(exactly = 1) { storageClient.write(any<String>(), any<String>()) }
    verify(exactly = 1) { statusManager.add(any<ErrorStatus>()) }
  }

  @Test
  fun testIdScrubbing() {
    val baseStorageId = "/path/to/logs/"
    val timestamp = "2024-01-01 00:00:00"
    val hostname = "localhost"
    val uniqueId = UUID.randomUUID().toString()

    val id = createFileId(baseId = baseStorageId, timestamp = timestamp, hostname = hostname, uniqueIdentifier = uniqueId)

    assertEquals("${baseStorageId.trim('/')}/${timestamp}_${hostname}_${uniqueId.replace("-","")}$STRUCTURED_LOG_FILE_EXTENSION", id)
  }
}
