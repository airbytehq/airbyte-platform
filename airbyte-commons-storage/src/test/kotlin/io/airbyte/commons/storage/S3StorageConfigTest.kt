/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.storage

import io.airbyte.commons.envvar.EnvVar
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
        // Audit logging is null by default as it is SME feature only
        auditLogging = null,
        profilerOutput = null,
        replicationDump = null,
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
    assertEquals(StorageType.S3.name, envVarMap[EnvVar.STORAGE_TYPE.name])
    assertEquals(accessKey, envVarMap[EnvVar.AWS_ACCESS_KEY_ID.name])
    assertEquals(secretAccessKey, envVarMap[EnvVar.AWS_SECRET_ACCESS_KEY.name])
    assertEquals(region, envVarMap[EnvVar.AWS_DEFAULT_REGION.name])
  }

  @Test
  internal fun testToEnvVarMapWithAuditLogging() {
    val accessKey = "access-key"
    val secretAccessKey = "secret-access-key"
    val region = Region.US_EAST_1.toString()
    val bucketConfig =
      StorageBucketConfig(
        state = "state",
        workloadOutput = "workload-output",
        log = "log",
        activityPayload = "activity-payload",
        auditLogging = "audit-logging",
        profilerOutput = null,
        replicationDump = null,
      )
    val s3StorageConfig =
      S3StorageConfig(
        buckets = bucketConfig,
        accessKey = accessKey,
        secretAccessKey = secretAccessKey,
        region = region,
      )
    val envVarMap = s3StorageConfig.toEnvVarMap()
    assertEquals(9, envVarMap.size)
    assertEquals(bucketConfig.log, envVarMap[EnvVar.STORAGE_BUCKET_LOG.name])
    assertEquals(bucketConfig.workloadOutput, envVarMap[EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT.name])
    assertEquals(bucketConfig.activityPayload, envVarMap[EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD.name])
    assertEquals(bucketConfig.state, envVarMap[EnvVar.STORAGE_BUCKET_STATE.name])
    assertEquals(bucketConfig.auditLogging, envVarMap[EnvVar.STORAGE_BUCKET_AUDIT_LOGGING.name])
    assertEquals(StorageType.S3.name, envVarMap[EnvVar.STORAGE_TYPE.name])
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
        auditLogging = "audit-logging",
        profilerOutput = null,
        replicationDump = null,
      )
    val s3StorageConfig =
      S3StorageConfig(
        buckets = bucketConfig,
        accessKey = null,
        secretAccessKey = null,
        region = region,
      )
    val envVarMap = s3StorageConfig.toEnvVarMap()
    assertEquals(7, envVarMap.size)
    assertEquals(bucketConfig.log, envVarMap[EnvVar.STORAGE_BUCKET_LOG.name])
    assertEquals(bucketConfig.workloadOutput, envVarMap[EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT.name])
    assertEquals(bucketConfig.activityPayload, envVarMap[EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD.name])
    assertEquals(bucketConfig.auditLogging, envVarMap[EnvVar.STORAGE_BUCKET_AUDIT_LOGGING.name])
    assertEquals(bucketConfig.state, envVarMap[EnvVar.STORAGE_BUCKET_STATE.name])
    assertEquals(StorageType.S3.name, envVarMap[EnvVar.STORAGE_TYPE.name])
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
        auditLogging = "audit-logging",
        profilerOutput = null,
        replicationDump = null,
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
