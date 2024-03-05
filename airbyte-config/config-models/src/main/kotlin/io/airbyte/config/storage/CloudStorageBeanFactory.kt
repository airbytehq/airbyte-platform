package io.airbyte.config.storage

import io.airbyte.config.storage.CloudStorageConfigs.GcsConfig
import io.airbyte.config.storage.CloudStorageConfigs.MinioConfig
import io.airbyte.config.storage.CloudStorageConfigs.S3Config
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton

/**
 * TODO: remove this class entirely, inject the [StorageConfig] instances directly instead.
 */
@Factory
class CloudStorageBeanFactory {
  @Singleton
  @Requires(property = "airbyte.cloud.storage.type", pattern = "(?i)^gcs$")
  @Named("stateStorageConfigs")
  fun gcsStateStorageConfiguration(storageConfig: StorageConfig): CloudStorageConfigs {
    if (storageConfig is GcsStorageConfig) {
      return CloudStorageConfigs.gcs(GcsConfig(storageConfig.buckets.state, storageConfig.applicationCredentials))
    }

    throw IllegalArgumentException("gcs storage defined but not provided - ${storageConfig::class.java}")
  }

  @Singleton
  @Requires(property = "airbyte.cloud.storage.type", pattern = "(?i)^minio$")
  @Named("stateStorageConfigs")
  fun minioStateStorageConfiguration(storageConfig: StorageConfig): CloudStorageConfigs {
    if (storageConfig is MinioStorageConfig) {
      return CloudStorageConfigs.minio(
        MinioConfig(storageConfig.buckets.state, storageConfig.accessKey, storageConfig.secretAccessKey, storageConfig.endpoint),
      )
    }

    throw IllegalArgumentException("minio storage defined but not provided - ${storageConfig::class.java}")
  }

  @Singleton
  @Requires(property = "airbyte.cloud.storage.type", pattern = "(?i)^s3$")
  @Named("stateStorageConfigs")
  fun s3StateStorageConfiguration(storageConfig: StorageConfig): CloudStorageConfigs {
    if (storageConfig is S3StorageConfig) {
      return CloudStorageConfigs.s3(
        S3Config(storageConfig.buckets.state, storageConfig.accessKey, storageConfig.secretAccessKey, storageConfig.region),
      )
    }

    throw IllegalArgumentException("s3 storage defined but not provided - ${storageConfig::class.java}")
  }
}
