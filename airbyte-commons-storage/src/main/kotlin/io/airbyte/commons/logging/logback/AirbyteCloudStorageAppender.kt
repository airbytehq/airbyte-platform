/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging.logback

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
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
import java.util.concurrent.TimeUnit

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
  private val encoder = AirbyteLogEventEncoder()

  private val uploader =
    AirbyteLogbackBulkUploader(
      baseStorageId = baseStorageId,
      storageClient = storageClient,
      period = period,
      unit = unit,
      encoder = encoder,
      addStatus = this::addStatus,
    )

  override fun start() {
    super.start()
    encoder.start()
    uploader.start()
  }

  override fun stop() {
    super.stop()
    encoder.stop()
    uploader.stop()
  }

  override fun append(eventObject: ILoggingEvent) {
    uploader.append(eventObject)
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
