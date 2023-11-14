/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.config.cloud

import io.micronaut.context.annotation.ConfigurationBuilder
import io.micronaut.context.annotation.ConfigurationProperties

@ConfigurationProperties("airbyte.cloud.storage.state")
class CloudStateConfig {
  var type = ""

  @ConfigurationBuilder(prefixes = ["with"], configurationPrefix = "gcs")
  val gcsCloudConfig = GcsCloudConfig.builder()

  @ConfigurationBuilder(prefixes = ["with"], configurationPrefix = "minio")
  val minioCloudLoggingConfig = MinioCloudConfig.builder()

  @ConfigurationBuilder(prefixes = ["with"], configurationPrefix = "s3")
  val s3CloudLoggingConfig = S3CloudConfig.builder()

  fun toEnvVarMap(): Map<String, String> {
    return if (type.isNotBlank()) {
      when (LoggingType.valueOf(type)) {
        LoggingType.GCS -> buildGcsConfig()
        LoggingType.MINIO -> buildMinoConfig()
        LoggingType.S3 -> buildS3Config()
      }
    } else {
      mapOf()
    }
  }

  private fun buildGcsConfig(): Map<String, String> {
    val gcsConfig = gcsCloudConfig.build()
    return mapOf(
      WORKER_STATE_STORAGE_TYPE to StateType.GCS.name,
      GCS_LOG_BUCKET_ENV_VAR to gcsConfig.bucket,
      GOOGLE_APPLICATION_CREDENTIALS to gcsConfig.applicationCredentials,
    )
  }

  private fun buildMinoConfig(): Map<String, String> {
    val minioConfig = minioCloudLoggingConfig.build()
    return mapOf(
      WORKER_STATE_STORAGE_TYPE to StateType.MINIO.name,
      AWS_ACCESS_KEY_ID_ENV_VAR to minioConfig.accessKey,
      S3_LOG_BUCKET_ENV_VAR to minioConfig.bucket,
      S3_MINIO_ENDPOINT_ENV_VAR to minioConfig.endpoint,
      AWS_SECRET_ACCESS_KEY_ENV_VAR to minioConfig.secretAccessKey,
    )
  }

  private fun buildS3Config(): Map<String, String> {
    val s3Config = s3CloudLoggingConfig.build()
    return mapOf(
      WORKER_STATE_STORAGE_TYPE to StateType.S3.name,
      AWS_ACCESS_KEY_ID_ENV_VAR to s3Config.accessKey,
      S3_LOG_BUCKET_ENV_VAR to s3Config.bucket,
      S3_LOG_BUCKET_REGION_ENV_VAR to s3Config.region,
      AWS_SECRET_ACCESS_KEY_ENV_VAR to s3Config.secretAccessKey,
    )
  }
  companion object {
    const val AWS_ACCESS_KEY_ID_ENV_VAR = "STATE_STORAGE_MINIO_ACCESS_KEY"
    const val S3_LOG_BUCKET_ENV_VAR = "STATE_STORAGE_S3_LOG_BUCKET"
    const val S3_MINIO_ENDPOINT_ENV_VAR = "STATE_STORAGE_S3_MINIO_ENDPOINT"
    const val AWS_SECRET_ACCESS_KEY_ENV_VAR = "STATE_STORAGE_AWS_SECRET_ACCESS_KEY"
    const val S3_LOG_BUCKET_REGION_ENV_VAR = "STATE_STORAGE_S3_LOG_BUCKET_REGION"
    const val WORKER_STATE_STORAGE_TYPE = "WORKER_STATE_STORAGE_TYPE"
    const val GCS_LOG_BUCKET_ENV_VAR = "STATE_STORAGE_GCS_BUCKET_NAME"
    const val GOOGLE_APPLICATION_CREDENTIALS = "STATE_STORAGE_GCS_APPLICATION_CREDENTIALS"
  }
}

enum class StateType {
  GCS,
  MINIO,
  S3,
}
