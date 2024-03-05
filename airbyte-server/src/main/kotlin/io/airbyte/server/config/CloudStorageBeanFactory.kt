package io.airbyte.server.config

import io.airbyte.config.storage.CloudStorageConfigs
import io.airbyte.config.storage.CloudStorageConfigs.GcsConfig
import io.airbyte.config.storage.CloudStorageConfigs.MinioConfig
import io.airbyte.config.storage.CloudStorageConfigs.S3Config
import io.airbyte.config.storage.GcsStorageConfig
import io.airbyte.config.storage.LocalStorageConfig
import io.airbyte.config.storage.MinioStorageConfig
import io.airbyte.config.storage.S3StorageConfig
import io.airbyte.config.storage.StorageConfig
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton

/**
 * TODO: remove this class entirely, inject the [StorageConfig] instances directly instead.
 */
@Factory
class CloudStorageBeanFactory {
//  @Singleton
//  @Requires(property = "airbyte.cloud.storage.type", pattern = "(?i)^gcs$")
//  @Named("logStorageConfigs")
//  fun gcsLogStorageConfigs(storageConfig: StorageConfig): CloudStorageConfigs {
//    if (storageConfig is GcsStorageConfig) {
//      return CloudStorageConfigs.gcs(GcsConfig(storageConfig.buckets.log, storageConfig.applicationCredentials))
//    }
//
//    throw IllegalArgumentException("gcs storage defined but not provided - ${storageConfig::class.java}")
//  }
//
//  @Singleton
//  @Requires(property = "airbyte.cloud.storage.type", pattern = "(?i)^minio$")
//  @Named("logStorageConfigs")
//  fun minioLogStorageConfigs(storageConfig: StorageConfig): CloudStorageConfigs {
//    if (storageConfig is MinioStorageConfig) {
//      return CloudStorageConfigs.minio(
//        MinioConfig(storageConfig.buckets.log, storageConfig.accessKey, storageConfig.secretAccessKey, storageConfig.endpoint),
//      )
//    }
//
//    throw IllegalArgumentException("minio storage defined but not provided - ${storageConfig::class.java}")
//  }
//
//  @Singleton
//  @Requires(property = "airbyte.cloud.storage.type", pattern = "(?i)^s3$")
//  @Named("logStorageConfigs")
//  fun s3LogStorageConfigs(storageConfig: StorageConfig): CloudStorageConfigs {
//    if (storageConfig is S3StorageConfig) {
//      return CloudStorageConfigs.s3(S3Config(storageConfig.buckets.log, storageConfig.accessKey, storageConfig.secretAccessKey, storageConfig.region))
//    }
//
//    throw IllegalArgumentException("s3 storage defined but not provided - ${storageConfig::class.java}")
//  }
//
//  @Singleton
//  @Requires(property = "airbyte.cloud.storage.type", pattern = "(?i)^local$")
//  @Named("logStorageConfigs")
//  fun localLogStorageConfigs(storageConfig: StorageConfig): CloudStorageConfigs {
//    if (storageConfig is LocalStorageConfig) {
//      return CloudStorageConfigs.local(CloudStorageConfigs.LocalConfig(storageConfig.root))
//    }
//
//    throw IllegalArgumentException("local storage defined but not provided - ${storageConfig::class.java}")
//  }

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

  @Singleton
  @Requires(property = "airbyte.cloud.storage.type", pattern = "(?i)^local$")
  @Named("stateStorageConfigs")
  fun localStateStorageConfiguration(storageConfig: StorageConfig): CloudStorageConfigs {
    if (storageConfig is LocalStorageConfig) {
      return CloudStorageConfigs.local(CloudStorageConfigs.LocalConfig(storageConfig.root))
    }

    throw IllegalArgumentException("local storage defined but not provided - ${storageConfig::class.java}")
  }
}
