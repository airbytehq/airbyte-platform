/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.storage

import io.airbyte.commons.envvar.EnvVar
import io.micronaut.context.annotation.Parameter
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton

interface StorageConfig {
  /**
   * The [StorageBucketConfig] settings that all implementations must utilize.
   */
  val buckets: StorageBucketConfig

  /**
   * Returns the environment variable mappings of the [StorageConfig].
   */
  fun toEnvVarMap(): Map<String, String>
}

/**
 * Bucket name configuration
 */
@Singleton
data class StorageBucketConfig(
  @Value("\${$STORAGE_BUCKET_LOG}") val log: String,
  @Value("\${$STORAGE_BUCKET_STATE}") val state: String,
  @Value("\${$STORAGE_BUCKET_WORKLOAD_OUTPUT}") val workloadOutput: String,
  @Value("\${$STORAGE_BUCKET_ACTIVITY_PAYLOAD}") val activityPayload: String,
  @Value("\${$STORAGE_BUCKET_AUDIT_LOGGING:}") val auditLogging: String?,
)

/**
 * Azure storage configuration
 *
// * @param applicationCredentials required credentials for accessing Azure Blob Storage
 */
@Singleton
@Requires(property = STORAGE_TYPE, pattern = "(?i)^azure$")
data class AzureStorageConfig(
  override val buckets: StorageBucketConfig,
  @Value("\${$STORAGE_AZURE.connection-string}") val connectionString: String,
) : StorageConfig {
  override fun toEnvVarMap(): Map<String, String> =
    buildMap {
      put(EnvVar.STORAGE_BUCKET_LOG, buckets.log)
      put(EnvVar.STORAGE_BUCKET_STATE, buckets.state)
      put(EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT, buckets.workloadOutput)
      put(EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD, buckets.activityPayload)
      buckets.auditLogging?.let {
        put(EnvVar.STORAGE_BUCKET_AUDIT_LOGGING, it)
      }
      put(EnvVar.STORAGE_TYPE, StorageType.AZURE.name)
      put(EnvVar.AZURE_STORAGE_CONNECTION_STRING, connectionString)
    }.mapKeys { it.key.name }

  override fun toString(): String = "AzureStorageConfig(connectionString=${connectionString.mask()})"
}

/**
 * GCS storage configuration
 *
 * @param applicationCredentials required credentials for accessing GCS
 */
@Singleton
@Requires(property = STORAGE_TYPE, pattern = "(?i)^gcs$")
data class GcsStorageConfig(
  override val buckets: StorageBucketConfig,
  @Value("\${$STORAGE_GCS.application-credentials}") val applicationCredentials: String,
) : StorageConfig {
  override fun toEnvVarMap(): Map<String, String> =
    buildMap {
      put(EnvVar.STORAGE_BUCKET_LOG, buckets.log)
      put(EnvVar.STORAGE_BUCKET_STATE, buckets.state)
      put(EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT, buckets.workloadOutput)
      put(EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD, buckets.activityPayload)
      buckets.auditLogging?.let {
        put(EnvVar.STORAGE_BUCKET_AUDIT_LOGGING, it)
      }
      put(EnvVar.STORAGE_TYPE, StorageType.GCS.name)
      put(EnvVar.GOOGLE_APPLICATION_CREDENTIALS, applicationCredentials)
    }.mapKeys { it.key.name }

  override fun toString(): String = "GcsStorageConfig(applicationCredentials=${applicationCredentials.mask()})"
}

/**
 * S3 storage configuration
 *
 * @param accessKey aws access key
 * @param secretAccessKey aws secret access key
 * @param region aws region
 */
@Singleton
@Requires(property = STORAGE_TYPE, pattern = "(?i)^s3$")
data class S3StorageConfig(
  override val buckets: StorageBucketConfig,
  @Value("\${$STORAGE_S3.access-key}") val accessKey: String?,
  @Value("\${$STORAGE_S3.secret-access-key}") val secretAccessKey: String?,
  @Value("\${$STORAGE_S3.region}") val region: String,
) : StorageConfig {
  override fun toEnvVarMap(): Map<String, String> =
    buildMap {
      put(EnvVar.STORAGE_BUCKET_LOG, buckets.log)
      put(EnvVar.STORAGE_BUCKET_STATE, buckets.state)
      put(EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT, buckets.workloadOutput)
      put(EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD, buckets.activityPayload)
      buckets.auditLogging?.let {
        put(EnvVar.STORAGE_BUCKET_AUDIT_LOGGING, it)
      }
      put(EnvVar.STORAGE_TYPE, StorageType.S3.name)
      accessKey?.let {
        put(EnvVar.AWS_ACCESS_KEY_ID, accessKey)
      }
      secretAccessKey?.let {
        put(EnvVar.AWS_SECRET_ACCESS_KEY, secretAccessKey)
      }
      put(EnvVar.AWS_DEFAULT_REGION, region)
    }.mapKeys { it.key.name }

  override fun toString(): String = "S3StorageConfig(accessKey=${accessKey.mask()}, secretAccessKey=${secretAccessKey.mask()}, region=$region)"
}

/**
 * MinIO storage configuration
 *
 * @param accessKey minio access key
 * @param secretAccessKey minio secret access key
 * @param endpoint minio endpoint
 */
@Singleton
@Requires(property = STORAGE_TYPE, pattern = "(?i)^minio$")
data class MinioStorageConfig(
  override val buckets: StorageBucketConfig,
  @Value("\${$STORAGE_MINIO.access-key}") val accessKey: String,
  @Value("\${$STORAGE_MINIO.secret-access-key}") val secretAccessKey: String,
  @Value("\${$STORAGE_MINIO.endpoint}") val endpoint: String,
) : StorageConfig {
  override fun toEnvVarMap(): Map<String, String> =
    buildMap {
      put(EnvVar.STORAGE_BUCKET_LOG, buckets.log)
      put(EnvVar.STORAGE_BUCKET_STATE, buckets.state)
      put(EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT, buckets.workloadOutput)
      put(EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD, buckets.activityPayload)
      buckets.auditLogging?.let {
        put(EnvVar.STORAGE_BUCKET_AUDIT_LOGGING, it)
      }
      put(EnvVar.STORAGE_TYPE, StorageType.MINIO.name)
      put(EnvVar.AWS_ACCESS_KEY_ID, accessKey)
      put(EnvVar.AWS_SECRET_ACCESS_KEY, secretAccessKey)
      put(EnvVar.MINIO_ENDPOINT, endpoint)
    }.mapKeys { it.key.name }

  override fun toString(): String = "MinioStorageConfig(accessKey=${accessKey.mask()}, secretAccessKey=${secretAccessKey.mask()}, endpoint=$endpoint)"
}

/**
 * Local storage configuration
 *
 * @param root root directory to use for logging
 */
@Singleton
@Requires(property = STORAGE_TYPE, pattern = "(?i)^local$")
class LocalStorageConfig(
  override val buckets: StorageBucketConfig,
  @Parameter val root: String = STORAGE_MOUNT,
) : StorageConfig {
  override fun toEnvVarMap(): Map<String, String> =
    buildMap {
      put(EnvVar.STORAGE_BUCKET_LOG, buckets.log)
      put(EnvVar.STORAGE_BUCKET_STATE, buckets.state)
      put(EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT, buckets.workloadOutput)
      put(EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD, buckets.activityPayload)
      buckets.auditLogging?.let {
        put(EnvVar.STORAGE_BUCKET_AUDIT_LOGGING, it)
      }
      put(EnvVar.STORAGE_TYPE, StorageType.LOCAL.name)
    }.mapKeys { it.key.name }
}

/**
 * Extension function to mask any sensitive data.
 *
 * Any non-null [String] that calls [mask] will return `"*******"`. Any null [String] will return `"null"`.
 */
private fun String?.mask(): String = this?.let { "*******" } ?: "null"
