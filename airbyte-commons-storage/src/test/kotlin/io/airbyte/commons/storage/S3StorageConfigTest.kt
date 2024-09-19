/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.storage

import io.airbyte.commons.envvar.EnvVar
import io.airbyte.commons.logging.LogClientType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test
import software.amazon.awssdk.regions.Region

internal class S3StorageConfigTest {
  @Test
  internal fun testToEnvVarMap() {
    val accessKey = "access-key"
    val secretAccessKey = "secret-access-key"
    val region = Region.US_EAST_1.toString()
    val bucketConfig =
      StorageBucketConfig(
        state = "state",
        workloadOutput = "workload-output",
        log = "log",
        activityPayload = "activity-payload",
      )
    val s3StorageConfig =
      S3StorageConfig(
        buckets = bucketConfig,
        accessKey = accessKey,
        secretAccessKey = secretAccessKey,
        region = region,
      )
    val envVarMap = s3StorageConfig.toEnvVarMap()
    assertEquals(8, envVarMap.size)
    assertEquals(bucketConfig.log, envVarMap[EnvVar.STORAGE_BUCKET_LOG.name])
    assertEquals(bucketConfig.workloadOutput, envVarMap[EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT.name])
    assertEquals(bucketConfig.activityPayload, envVarMap[EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD.name])
    assertEquals(bucketConfig.state, envVarMap[EnvVar.STORAGE_BUCKET_STATE.name])
    assertEquals(LogClientType.S3.name, envVarMap[EnvVar.STORAGE_TYPE.name])
    assertEquals(accessKey, envVarMap[EnvVar.AWS_ACCESS_KEY_ID.name])
    assertEquals(secretAccessKey, envVarMap[EnvVar.AWS_SECRET_ACCESS_KEY.name])
    assertEquals(region, envVarMap[EnvVar.AWS_DEFAULT_REGION.name])
  }

  @Test
  internal fun testToEnvVarMapBlankCredentials() {
    val region = Region.US_EAST_1.toString()
    val bucketConfig =
      StorageBucketConfig(
        state = "state",
        workloadOutput = "workload-output",
        log = "log",
        activityPayload = "activity-payload",
      )
    val s3StorageConfig =
      S3StorageConfig(
        buckets = bucketConfig,
        accessKey = null,
        secretAccessKey = null,
        region = region,
      )
    val envVarMap = s3StorageConfig.toEnvVarMap()
    assertEquals(6, envVarMap.size)
    assertEquals(bucketConfig.log, envVarMap[EnvVar.STORAGE_BUCKET_LOG.name])
    assertEquals(bucketConfig.workloadOutput, envVarMap[EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT.name])
    assertEquals(bucketConfig.activityPayload, envVarMap[EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD.name])
    assertEquals(bucketConfig.state, envVarMap[EnvVar.STORAGE_BUCKET_STATE.name])
    assertEquals(LogClientType.S3.name, envVarMap[EnvVar.STORAGE_TYPE.name])
    assertFalse(envVarMap.containsKey(EnvVar.AWS_ACCESS_KEY_ID.name))
    assertFalse(envVarMap.containsKey(EnvVar.AWS_SECRET_ACCESS_KEY.name))
    assertEquals(region, envVarMap[EnvVar.AWS_DEFAULT_REGION.name])
  }

  @Test
  internal fun testToString() {
    val accessKey = "access-key"
    val secretAccessKey = "secret-access-key"
    val region = Region.US_EAST_1.toString()
    val bucketConfig =
      StorageBucketConfig(
        state = "state",
        workloadOutput = "workload-output",
        log = "log",
        activityPayload = "activity-payload",
      )
    val s3StorageConfig =
      S3StorageConfig(
        buckets = bucketConfig,
        accessKey = accessKey,
        secretAccessKey = secretAccessKey,
        region = region,
      )
    val toString = s3StorageConfig.toString()
    assertEquals("S3StorageConfig(accessKey=*******, secretAccessKey=*******, region=$region)", toString)
  }
}
