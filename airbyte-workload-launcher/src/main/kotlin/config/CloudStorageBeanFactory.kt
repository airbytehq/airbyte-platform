package io.airbyte.workload.launcher.config

import io.airbyte.config.storage.CloudStorageConfigs
import io.airbyte.config.storage.CloudStorageConfigs.GcsConfig
import io.airbyte.config.storage.CloudStorageConfigs.MinioConfig
import io.airbyte.config.storage.CloudStorageConfigs.S3Config
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton

/**
 * Micronaut bean factory for cloud storage-related singletons.
 */
@Factory
class CloudStorageBeanFactory {
// TODO: do we need any of this log stuff?
//  @Singleton
//  @Requires(property = "airbyte.cloud.storage.logs.type", pattern = "(?i)^gcs$")
//  @Named("logStorageConfigs")
//  fun gcsLogStorageConfigs(
//    @Value("\${airbyte.cloud.storage.logs.gcs.bucket}") gcsLogBucket: String?,
//    @Value("\${airbyte.cloud.storage.logs.gcs.application-credentials}") googleApplicationCredentials: String?): CloudStorageConfigs {
//    return CloudStorageConfigs.gcs(GcsConfig(gcsLogBucket, googleApplicationCredentials))
//  }
//
//  @Singleton
//  @Requires(property = "airbyte.cloud.storage.logs.type", pattern = "(?i)^minio$")
//  @Named("logStorageConfigs")
//  fun minioLogStorageConfigs(
//    @Value("\${airbyte.cloud.storage.logs.minio.access-key}") awsAccessKeyId: String?,
//    @Value("\${airbyte.cloud.storage.logs.minio.secret-access-key}") awsSecretAccessKey: String?,
//    @Value("\${airbyte.cloud.storage.logs.minio.bucket}") s3LogBucket: String?,
//    @Value("\${airbyte.cloud.storage.logs.minio.endpoint}") s3MinioEndpoint: String?): CloudStorageConfigs {
//    return CloudStorageConfigs.minio(MinioConfig(s3LogBucket, awsAccessKeyId, awsSecretAccessKey, s3MinioEndpoint))
//  }
//
//  @Singleton
//  @Requires(property = "airbyte.cloud.storage.logs.type", pattern = "(?i)^s3$")
//  @Named("logStorageConfigs")
//  fun s3LogStorageConfigs(
//    @Value("\${airbyte.cloud.storage.logs.s3.access-key}") awsAccessKeyId: String?,
//    @Value("\${airbyte.cloud.storage.logs.s3.secret-access-key}") awsSecretAccessKey: String?,
//    @Value("\${airbyte.cloud.storage.logs.s3.bucket}") s3LogBucket: String?,
//    @Value("\${airbyte.cloud.storage.logs.s3.region}") s3LogBucketRegion: String?): CloudStorageConfigs {
//    return CloudStorageConfigs.s3(S3Config(s3LogBucket, awsAccessKeyId, awsSecretAccessKey, s3LogBucketRegion))
//  }

  @Singleton
  @Requires(property = "airbyte.cloud.storage.state.type", pattern = "(?i)^gcs$")
  @Named("stateStorageConfigs")
  fun gcsStateStorageConfiguration(
    @Value("\${airbyte.cloud.storage.state.gcs.bucket}") gcsBucketName: String?,
    @Value("\${airbyte.cloud.storage.state.gcs.application-credentials}") gcsApplicationCredentials: String?,
  ): CloudStorageConfigs {
    return CloudStorageConfigs.gcs(GcsConfig(gcsBucketName, gcsApplicationCredentials))
  }

  @Singleton
  @Requires(property = "airbyte.cloud.storage.state.type", pattern = "(?i)^minio$")
  @Named("stateStorageConfigs")
  fun minioStateStorageConfiguration(
    @Value("\${airbyte.cloud.storage.state.minio.bucket}") bucketName: String?,
    @Value("\${airbyte.cloud.storage.state.minio.access-key}") awsAccessKey: String?,
    @Value("\${airbyte.cloud.storage.state.minio.secret-access-key}") secretAccessKey: String?,
    @Value("\${airbyte.cloud.storage.state.minio.endpoint}") endpoint: String?,
  ): CloudStorageConfigs {
    return CloudStorageConfigs.minio(MinioConfig(bucketName, awsAccessKey, secretAccessKey, endpoint))
  }

  @Singleton
  @Requires(property = "airbyte.cloud.storage.state.type", pattern = "(?i)^s3$")
  @Named("stateStorageConfigs")
  fun s3StateStorageConfiguration(
    @Value("\${airbyte.cloud.storage.state.s3.bucket}") bucketName: String?,
    @Value("\${airbyte.cloud.storage.state.s3.access-key}") awsAccessKey: String?,
    @Value("\${airbyte.cloud.storage.state.s3.secret-access-key}") secretAcessKey: String?,
    @Value("\${airbyte.cloud.storage.state.s3.region}") s3Region: String?,
  ): CloudStorageConfigs {
    return CloudStorageConfigs.s3(S3Config(bucketName, awsAccessKey, secretAcessKey, s3Region))
  }
}
