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
  val minioCloudConfig = MinioCloudConfig.builder()

  @ConfigurationBuilder(prefixes = ["with"], configurationPrefix = "s3")
  val s3CloudConfig = S3CloudConfig.builder()

  fun toEnvVarMap(): Map<String, String> {
    return if (type.isNotBlank()) {
      try {
        when (LoggingType.valueOf(type)) {
          LoggingType.GCS -> buildGcsConfig()
          LoggingType.MINIO -> buildMinoConfig()
          LoggingType.S3 -> buildS3Config()
        }
      } catch (e: IllegalArgumentException) {
        mapOf()
      }
    } else {
      mapOf()
    }
  }

  private fun buildGcsConfig(): Map<String, String> {
    val gcsConfig = gcsCloudConfig.build()
    return mapOf(
      WORKER_STATE_STORAGE_TYPE_ENV_VAR to StateType.GCS.name,
      GCS_STATE_BUCKET_ENV_VAR to gcsConfig.bucket,
      GOOGLE_APPLICATION_CREDENTIALS_ENV_VAR to gcsConfig.applicationCredentials,
    )
  }

  private fun buildMinoConfig(): Map<String, String> {
    val minioConfig = minioCloudConfig.build()
    return mapOf(
      WORKER_STATE_STORAGE_TYPE_ENV_VAR to StateType.MINIO.name,
      AWS_ACCESS_KEY_ID_ENV_VAR to minioConfig.accessKey,
      S3_STATE_BUCKET_ENV_VAR to minioConfig.bucket,
      S3_MINIO_ENDPOINT_ENV_VAR to minioConfig.endpoint,
      AWS_SECRET_ACCESS_KEY_ENV_VAR to minioConfig.secretAccessKey,
    )
  }

  private fun buildS3Config(): Map<String, String> {
    val s3Config = s3CloudConfig.build()
    return mapOf(
      WORKER_STATE_STORAGE_TYPE_ENV_VAR to StateType.S3.name,
      AWS_ACCESS_KEY_ID_ENV_VAR to s3Config.accessKey,
      S3_STATE_BUCKET_ENV_VAR to s3Config.bucket,
      S3_STATE_BUCKET_REGION_ENV_VAR to s3Config.region,
      AWS_SECRET_ACCESS_KEY_ENV_VAR to s3Config.secretAccessKey,
    )
  }
  companion object {
    const val AWS_ACCESS_KEY_ID_ENV_VAR = "STATE_STORAGE_MINIO_ACCESS_KEY"
    const val AWS_SECRET_ACCESS_KEY_ENV_VAR = "STATE_STORAGE_AWS_SECRET_ACCESS_KEY"
    const val GCS_STATE_BUCKET_ENV_VAR = "STATE_STORAGE_GCS_BUCKET_NAME"
    const val GOOGLE_APPLICATION_CREDENTIALS_ENV_VAR = "STATE_STORAGE_GCS_APPLICATION_CREDENTIALS"
    const val S3_STATE_BUCKET_ENV_VAR = "STATE_STORAGE_S3_LOG_BUCKET"
    const val S3_STATE_BUCKET_REGION_ENV_VAR = "STATE_STORAGE_S3_LOG_BUCKET_REGION"
    const val S3_MINIO_ENDPOINT_ENV_VAR = "STATE_STORAGE_S3_MINIO_ENDPOINT"
    const val WORKER_STATE_STORAGE_TYPE_ENV_VAR = "WORKER_STATE_STORAGE_TYPE"
  }
}

enum class StateType {
  GCS,
  MINIO,
  S3,
}
