/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.config.cloud

import io.airbyte.workload.launcher.config.cloud.CloudStateConfig.Companion.AWS_ACCESS_KEY_ID_ENV_VAR
import io.airbyte.workload.launcher.config.cloud.CloudStateConfig.Companion.AWS_SECRET_ACCESS_KEY_ENV_VAR
import io.airbyte.workload.launcher.config.cloud.CloudStateConfig.Companion.GCS_STATE_BUCKET_ENV_VAR
import io.airbyte.workload.launcher.config.cloud.CloudStateConfig.Companion.GOOGLE_APPLICATION_CREDENTIALS_ENV_VAR
import io.airbyte.workload.launcher.config.cloud.CloudStateConfig.Companion.S3_MINIO_ENDPOINT_ENV_VAR
import io.airbyte.workload.launcher.config.cloud.CloudStateConfig.Companion.S3_STATE_BUCKET_ENV_VAR
import io.airbyte.workload.launcher.config.cloud.CloudStateConfig.Companion.S3_STATE_BUCKET_REGION_ENV_VAR
import io.airbyte.workload.launcher.config.cloud.CloudStateConfig.Companion.WORKER_STATE_STORAGE_TYPE_ENV_VAR
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class CloudStateConfigTest {
  @Test
  internal fun `test the population of the GCS cloud state configuration from properties`() {
    val applicationCredentials = "credentials"
    val bucket = "bucket"
    val type = StateType.GCS.name
    val config = CloudStateConfig()
    config.type = type
    config.gcsCloudConfig.withApplicationCredentials(applicationCredentials).withBucket(bucket)

    val envVars = config.toEnvVarMap()
    assertEquals(3, envVars.size)
    assertEquals(type, envVars[WORKER_STATE_STORAGE_TYPE_ENV_VAR])
    assertEquals(applicationCredentials, envVars[GOOGLE_APPLICATION_CREDENTIALS_ENV_VAR])
    assertEquals(bucket, envVars[GCS_STATE_BUCKET_ENV_VAR])
  }

  @Test
  internal fun `test the population of the Minio cloud state configuration from properties`() {
    val accessKey = "access-key"
    val bucket = "bucket"
    val endpoint = "endpoint"
    val secretAccessKey = "secret-access-key"
    val type = StateType.MINIO.name
    val config = CloudStateConfig()
    config.type = type
    config.minioCloudConfig.withAccessKey(accessKey).withBucket(bucket).withEndpoint(endpoint).withSecretAccessKey(secretAccessKey)

    val envVars = config.toEnvVarMap()
    assertEquals(5, envVars.size)
    assertEquals(type, envVars[WORKER_STATE_STORAGE_TYPE_ENV_VAR])
    assertEquals(accessKey, envVars[AWS_ACCESS_KEY_ID_ENV_VAR])
    assertEquals(bucket, envVars[S3_STATE_BUCKET_ENV_VAR])
    assertEquals(endpoint, envVars[S3_MINIO_ENDPOINT_ENV_VAR])
    assertEquals(secretAccessKey, envVars[AWS_SECRET_ACCESS_KEY_ENV_VAR])
  }

  @Test
  internal fun `test the population of the S3 cloud state configuration from properties`() {
    val accessKey = "access-key"
    val bucket = "bucket"
    val region = "region"
    val secretAccessKey = "secret-access-key"
    val type = StateType.S3.name
    val config = CloudStateConfig()
    config.type = type
    config.s3CloudConfig.withAccessKey(accessKey).withBucket(bucket).withRegion(region).withSecretAccessKey(secretAccessKey)

    val envVars = config.toEnvVarMap()
    assertEquals(5, envVars.size)
    assertEquals(type, envVars[WORKER_STATE_STORAGE_TYPE_ENV_VAR])
    assertEquals(accessKey, envVars[AWS_ACCESS_KEY_ID_ENV_VAR])
    assertEquals(bucket, envVars[S3_STATE_BUCKET_ENV_VAR])
    assertEquals(region, envVars[S3_STATE_BUCKET_REGION_ENV_VAR])
    assertEquals(secretAccessKey, envVars[AWS_SECRET_ACCESS_KEY_ENV_VAR])
  }

  @Test
  internal fun `test that an unknown or blank type is handled`() {
    val config = CloudStateConfig()
    config.type = "unknown"

    val envVars = config.toEnvVarMap()
    assertEquals(0, envVars.size)

    config.type = ""

    val envVars2 = config.toEnvVarMap()
    assertEquals(0, envVars2.size)
  }
}
