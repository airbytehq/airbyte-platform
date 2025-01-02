/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.storage

import io.airbyte.commons.envvar.EnvVar
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class MinioStorageConfigTest {
  @Test
  internal fun testToEnvVarMap() {
    val accessKey = "access-key"
    val secretAccessKey = "secret-access-key"
    val endpoint = "http://localhost:8080"
    val bucketConfig =
      StorageBucketConfig(
        state = "state",
        workloadOutput = "workload-output",
        log = "log",
        activityPayload = "activity-payload",
        // Audit logging is null by default as it is SME feature only
        auditLogging = null,
      )
    val s3StorageConfig =
      MinioStorageConfig(
        buckets = bucketConfig,
        accessKey = accessKey,
        secretAccessKey = secretAccessKey,
        endpoint = endpoint,
      )
    val envVarMap = s3StorageConfig.toEnvVarMap()
    assertEquals(8, envVarMap.size)
    assertEquals(bucketConfig.log, envVarMap[EnvVar.STORAGE_BUCKET_LOG.name])
    assertEquals(bucketConfig.workloadOutput, envVarMap[EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT.name])
    assertEquals(bucketConfig.activityPayload, envVarMap[EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD.name])
    assertEquals(bucketConfig.state, envVarMap[EnvVar.STORAGE_BUCKET_STATE.name])
    assertEquals(StorageType.MINIO.name, envVarMap[EnvVar.STORAGE_TYPE.name])
    assertEquals(accessKey, envVarMap[EnvVar.AWS_ACCESS_KEY_ID.name])
    assertEquals(secretAccessKey, envVarMap[EnvVar.AWS_SECRET_ACCESS_KEY.name])
    assertEquals(endpoint, envVarMap[EnvVar.MINIO_ENDPOINT.name])
  }

  @Test
  internal fun testToEnvVarMapWithAuditLogging() {
    val accessKey = "access-key"
    val secretAccessKey = "secret-access-key"
    val endpoint = "http://localhost:8080"
    val bucketConfig =
      StorageBucketConfig(
        state = "state",
        workloadOutput = "workload-output",
        log = "log",
        activityPayload = "activity-payload",
        auditLogging = "audit-logging",
      )
    val s3StorageConfig =
      MinioStorageConfig(
        buckets = bucketConfig,
        accessKey = accessKey,
        secretAccessKey = secretAccessKey,
        endpoint = endpoint,
      )
    val envVarMap = s3StorageConfig.toEnvVarMap()
    assertEquals(9, envVarMap.size)
    assertEquals(bucketConfig.log, envVarMap[EnvVar.STORAGE_BUCKET_LOG.name])
    assertEquals(bucketConfig.workloadOutput, envVarMap[EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT.name])
    assertEquals(bucketConfig.activityPayload, envVarMap[EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD.name])
    assertEquals(bucketConfig.state, envVarMap[EnvVar.STORAGE_BUCKET_STATE.name])
    assertEquals(bucketConfig.auditLogging, envVarMap[EnvVar.STORAGE_BUCKET_AUDIT_LOGGING.name])
    assertEquals(StorageType.MINIO.name, envVarMap[EnvVar.STORAGE_TYPE.name])
    assertEquals(accessKey, envVarMap[EnvVar.AWS_ACCESS_KEY_ID.name])
    assertEquals(secretAccessKey, envVarMap[EnvVar.AWS_SECRET_ACCESS_KEY.name])
    assertEquals(endpoint, envVarMap[EnvVar.MINIO_ENDPOINT.name])
  }

  @Test
  internal fun testToString() {
    val accessKey = "access-key"
    val secretAccessKey = "secret-access-key"
    val endpoint = "http://localhost:8080"
    val bucketConfig =
      StorageBucketConfig(
        state = "state",
        workloadOutput = "workload-output",
        log = "log",
        activityPayload = "activity-payload",
        auditLogging = "audit-logging",
      )
    val s3StorageConfig =
      MinioStorageConfig(
        buckets = bucketConfig,
        accessKey = accessKey,
        secretAccessKey = secretAccessKey,
        endpoint = endpoint,
      )
    val toString = s3StorageConfig.toString()
    assertEquals("MinioStorageConfig(accessKey=*******, secretAccessKey=*******, endpoint=$endpoint)", toString)
  }
}
