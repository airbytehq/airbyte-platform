/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging.logback

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import ch.qos.logback.core.status.ErrorStatus
import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.airbyte.commons.envvar.EnvVar
import io.airbyte.commons.storage.AzureStorageClient
import io.airbyte.commons.storage.AzureStorageConfig
import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.GcsStorageClient
import io.airbyte.commons.storage.GcsStorageConfig
import io.airbyte.commons.storage.LocalStorageClient
import io.airbyte.commons.storage.LocalStorageConfig
import io.airbyte.commons.storage.MinioStorageClient
import io.airbyte.commons.storage.MinioStorageConfig
import io.airbyte.commons.storage.S3StorageClient
import io.airbyte.commons.storage.S3StorageConfig
import io.airbyte.commons.storage.StorageBucketConfig
import io.airbyte.commons.storage.StorageClient
import java.net.InetAddress
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit

/**
 * Builds the ID of the uploaded file.  This is typically the path in blob storage.
 *
 * @param baseId The base path/ID of the file location
 * @param timestamp A timestamp as a string for uniqueness
 * @param hostname The hostname of the machine executing this method
 * @param uniqueIdentifier A random UUID as a string for uniqueness
 * @return The field ID.
 */
fun createFileId(
  baseId: String,
  timestamp: String = LocalDateTime.now().format(DATE_FORMAT),
  hostname: String = InetAddress.getLocalHost().hostName,
  uniqueIdentifier: String = UUID.randomUUID().toString(),
): String {
  // Remove the leading/trailing "/" from the base storage ID if present to avoid duplicates in the storage ID
  return "${baseId.trim('/')}/${timestamp}_${hostname}_${uniqueIdentifier.replace("-", "")}$STRUCTURED_LOG_FILE_EXTENSION"
}

object AirbyteCloudStorageAppenderExecutorServiceHelper {
  /**
   * Shared executor service used to reduce the number of threads created to handle
   * uploading log data to remote storage.
   */
  private val executorService =
    Executors.newScheduledThreadPool(
      EnvVar.CLOUD_STORAGE_APPENDER_THREADS.fetch(default = "20")!!.toInt(),
      ThreadFactoryBuilder().setNameFormat("airbyte-cloud-storage-appender-%d").build(),
    )

  /**
   * Schedules a runnable task on the underlying executor service.
   *
   * @param runnable The task to be executed.
   * @param initDelay The time to delay first execution.
   * @param period The period between successive executions.
   * @param unit The time unit of the initialDelay and period parameters
   * @return A [ScheduledFuture] representing pending completion of the series of repeated tasks.
   */
  fun scheduleTask(
    runnable: Runnable,
    initDelay: Long,
    period: Long,
    unit: TimeUnit,
  ): ScheduledFuture<*> = executorService.scheduleAtFixedRate(runnable, initDelay, period, unit)

  /**
   * Stops the shared executor service.  This method should be called from a JVM shutdown hook
   * to ensure that the thread pool is stopped prior to exit/stopping the appenders.
   */
  fun stopAirbyteCloudStorageAppenderExecutorService() {
    executorService.shutdownNow()
    executorService.awaitTermination(30, TimeUnit.SECONDS)
  }

  init {
    // Enable cancellation of tasks to prevent executor queue from growing unbounded
    if (executorService is ScheduledThreadPoolExecutor) {
      executorService.removeOnCancelPolicy = true
    }
  }
}

/**
 * Custom Logback [AppenderBase] that uploads log events to remove storage.  Log data
 * is uploaded on a scheduled cadence that produces a new remote storage file each time.
 * This is necessary because most cloud storage systems do not support an append mode.
 */
class AirbyteCloudStorageAppender(
  val baseStorageId: String,
  val documentType: DocumentType,
  val storageClient: StorageClient = buildStorageClient(storageConfig = buildStorageConfig(), documentType = documentType),
  val period: Long = 60L,
  val unit: TimeUnit = TimeUnit.SECONDS,
) : AppenderBase<ILoggingEvent>() {
  private val buffer = LinkedBlockingQueue<ILoggingEvent>()
  private var currentStorageId: String = createFileId(baseId = baseStorageId)
  private val encoder = AirbyteLogEventEncoder()
  private val uploadLock = Any()
  private lateinit var uploadTask: ScheduledFuture<*>

  override fun start() {
    super.start()
    encoder.start()
    uploadTask = AirbyteCloudStorageAppenderExecutorServiceHelper.scheduleTask(this::upload, period, period, unit)
  }

  override fun stop() {
    try {
      super.stop()
      uploadTask.cancel(false)
    } finally {
      // Do one final upload attempt to ensure that all logs are published
      upload()
      encoder.stop()
    }
  }

  override fun append(eventObject: ILoggingEvent) {
    buffer.offer(eventObject)
  }

  private fun upload() {
    try {
      synchronized(uploadLock) {
        val events = mutableListOf<ILoggingEvent>()
        buffer.drainTo(events)

        if (events.isNotEmpty()) {
          val document = encoder.bulkEncode(loggingEvents = events)
          storageClient.write(id = currentStorageId, document = document)

          // Move to next file to avoid overwriting in log storage that doesn't support append mode
          this.currentStorageId = createFileId(baseId = baseStorageId)
        }
      }
    } catch (t: Throwable) {
      addStatus(ErrorStatus("Failed to upload logs to cloud storage location $currentStorageId.", this, t))
    }
  }
}

internal fun buildStorageClient(
  documentType: DocumentType,
  storageConfig: Map<EnvVar, String>,
): StorageClient {
  val storageType = storageConfig[EnvVar.STORAGE_TYPE] ?: ""
  val bucketConfig = buildBucketConfig(storageConfig = storageConfig)

  return when (storageType.lowercase()) {
    "azure" ->
      AzureStorageClient(
        config =
          AzureStorageConfig(
            buckets = bucketConfig,
            connectionString = storageConfig[EnvVar.AZURE_STORAGE_CONNECTION_STRING]!!,
          ),
        type = documentType,
      )
    "gcs" ->
      GcsStorageClient(
        config =
          GcsStorageConfig(
            buckets = bucketConfig,
            applicationCredentials = storageConfig[EnvVar.GOOGLE_APPLICATION_CREDENTIALS]!!,
          ),
        type = documentType,
      )
    "minio" ->
      MinioStorageClient(
        config =
          MinioStorageConfig(
            buckets = bucketConfig,
            accessKey = storageConfig[EnvVar.AWS_ACCESS_KEY_ID]!!,
            secretAccessKey = storageConfig[EnvVar.AWS_SECRET_ACCESS_KEY]!!,
            endpoint = storageConfig[EnvVar.MINIO_ENDPOINT]!!,
          ),
        type = documentType,
      )
    "s3" ->
      S3StorageClient(
        config =
          S3StorageConfig(
            buckets = bucketConfig,
            accessKey = storageConfig[EnvVar.AWS_ACCESS_KEY_ID]!!,
            secretAccessKey = storageConfig[EnvVar.AWS_SECRET_ACCESS_KEY]!!,
            region = storageConfig[EnvVar.AWS_DEFAULT_REGION]!!,
          ),
        type = documentType,
      )
    else ->
      LocalStorageClient(
        config =
          LocalStorageConfig(
            buckets = bucketConfig,
          ),
        type = documentType,
      )
  }
}

const val STRUCTURED_LOG_FILE_EXTENSION = ".json"
private val DATE_FORMAT: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

internal fun buildBucketConfig(storageConfig: Map<EnvVar, String>): StorageBucketConfig =
  StorageBucketConfig(
    log = storageConfig[EnvVar.STORAGE_BUCKET_LOG] ?: throw IllegalArgumentException("Missing ${EnvVar.STORAGE_BUCKET_LOG.name} env-var"),
    state = "",
    workloadOutput = "",
    activityPayload = "",
    auditLogging = storageConfig[EnvVar.STORAGE_BUCKET_AUDIT_LOGGING] ?: "",
    profilerOutput = "",
  )

private fun buildStorageConfig(): Map<EnvVar, String> =
  mapOf(
    EnvVar.STORAGE_TYPE to EnvVar.STORAGE_TYPE.fetchNotNull(),
    EnvVar.STORAGE_BUCKET_LOG to EnvVar.STORAGE_BUCKET_LOG.fetchNotNull(),
    EnvVar.STORAGE_BUCKET_AUDIT_LOGGING to EnvVar.STORAGE_BUCKET_AUDIT_LOGGING.fetchNotNull(),
    EnvVar.AZURE_STORAGE_CONNECTION_STRING to EnvVar.AZURE_STORAGE_CONNECTION_STRING.fetchNotNull(),
    EnvVar.GOOGLE_APPLICATION_CREDENTIALS to EnvVar.GOOGLE_APPLICATION_CREDENTIALS.fetchNotNull(),
    EnvVar.AWS_ACCESS_KEY_ID to EnvVar.AWS_ACCESS_KEY_ID.fetchNotNull(),
    EnvVar.AWS_SECRET_ACCESS_KEY to EnvVar.AWS_SECRET_ACCESS_KEY.fetchNotNull(),
    EnvVar.AWS_DEFAULT_REGION to EnvVar.AWS_DEFAULT_REGION.fetchNotNull(),
    EnvVar.MINIO_ENDPOINT to EnvVar.MINIO_ENDPOINT.fetchNotNull(),
  )
