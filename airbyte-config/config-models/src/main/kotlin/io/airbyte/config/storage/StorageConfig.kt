package io.airbyte.config.storage

import io.airbyte.commons.envvar.EnvVar
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton

/** The root node which contains the configuration settings for the [StorageConfig]. */
internal const val STORAGE_ROOT = "airbyte.cloud.storage"

/** Specific settings for Google Cloud Storage are to be found here. Only used if `[STORAGE_ROOT].type` is `gcs`. */
internal const val STORAGE_GCS = "$STORAGE_ROOT.gcs"

/** Specific settings for local storage are to be found here. Only used if `[STORAGE_ROOT].type` is `local`. */
internal const val STORAGE_LOCAL = "$STORAGE_ROOT.local"

/** Specific settings for MinIO are to be found here. Only used if `[STORAGE_ROOT].type` is `minio`. */
internal const val STORAGE_MINIO = "$STORAGE_ROOT.minio"

/** Specific settings for S3 are to be found here. Only used if `[STORAGE_ROOT].type` is `s3`. */
internal const val STORAGE_S3 = "$STORAGE_ROOT.s3"

/** Specific settings for buckets, specifically their names. */
internal const val STORAGE_BUCKET = "$STORAGE_ROOT.bucket"

/** The setting that contains why [StorageType] this [StorageConfig] represents. */
const val STORAGE_TYPE = "$STORAGE_ROOT.type"

/**
 * Supported storage type.
 */
enum class StorageType {
  GCS,
  LOCAL,
  MINIO,
  S3,
}

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
  @Value("\${$STORAGE_LOCAL.root}") val root: String,
) : StorageConfig {
  override fun toEnvVarMap(): Map<String, String> =
    buildMap {
      put(EnvVar.STORAGE_BUCKET_LOG, buckets.log)
      put(EnvVar.STORAGE_BUCKET_STATE, buckets.state)
      put(EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT, buckets.workloadOutput)
      put(EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD, buckets.activityPayload)
      put(EnvVar.STORAGE_TYPE, StorageType.LOCAL.name)
      put(EnvVar.LOCAL_ROOT, root)
    }.mapKeys { it.key.name }
}

/**
 * Bucket name configuration
 */
@Singleton
data class StorageBucketConfig(
  @Value("\${$STORAGE_BUCKET.log}") val log: String,
  @Value("\${$STORAGE_BUCKET.state}") val state: String,
  @Value("\${$STORAGE_BUCKET.workload-output}") val workloadOutput: String,
  @Value("\${$STORAGE_BUCKET.activity-payload}") val activityPayload: String,
)

/**
 * Extension function to mask any sensitive data.
 *
 * Any non-null [String] that calls [mask] will return `"*******"`. Any null [String] will return `"null"`.
 */
private fun String?.mask(): String = this?.let { "*******" } ?: "null"
