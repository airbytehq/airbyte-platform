/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.storage

import io.airbyte.commons.envvar.EnvVar
import io.airbyte.micronaut.runtime.AirbyteStorageConfig
import io.airbyte.micronaut.runtime.StorageEnvironmentVariableProvider
import io.airbyte.micronaut.runtime.StorageType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import software.amazon.awssdk.regions.Region

internal class S3StorageConfigTest {
  @Test
  internal fun testToEnvVarMap() {
    val accessKey = "access-key"
    val secretAccessKey = "secret-access-key"
    val region = Region.US_EAST_1.toString()
    val bucketConfig =
      AirbyteStorageConfig.AirbyteStorageBucketConfig(
        state = "state",
        workloadOutput = "workload-output",
        log = "log",
        activityPayload = "activity-payload",
        // Audit logging is null by default as it is SME feature only
        auditLogging = "",
        profilerOutput = "",
        replicationDump = "",
      )
    val s3StorageConfig =
      AirbyteStorageConfig.S3StorageConfig(
        accessKey = accessKey,
        secretAccessKey = secretAccessKey,
        region = region,
      )
    val storageEnvironmentVariableProvider =
      StorageEnvironmentVariableProvider(
        buckets = bucketConfig,
        storageConfig = s3StorageConfig,
      )
    val envVarMap = storageEnvironmentVariableProvider.toEnvVarMap()
    assertEquals(10, envVarMap.size)
    assertEquals(bucketConfig.log, envVarMap[EnvVar.STORAGE_BUCKET_LOG.name])
    assertEquals(bucketConfig.workloadOutput, envVarMap[EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT.name])
    assertEquals(bucketConfig.activityPayload, envVarMap[EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD.name])
    assertEquals(bucketConfig.state, envVarMap[EnvVar.STORAGE_BUCKET_STATE.name])
    assertEquals(bucketConfig.auditLogging, envVarMap[EnvVar.STORAGE_BUCKET_AUDIT_LOGGING.name])
    assertEquals(bucketConfig.replicationDump, envVarMap[EnvVar.STORAGE_BUCKET_REPLICATION_DUMP.name])
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
      AirbyteStorageConfig.AirbyteStorageBucketConfig(
        state = "state",
        workloadOutput = "workload-output",
        log = "log",
        activityPayload = "activity-payload",
        auditLogging = "audit-logging",
        profilerOutput = "",
        replicationDump = "",
      )
    val s3StorageConfig =
      AirbyteStorageConfig.S3StorageConfig(
        accessKey = accessKey,
        secretAccessKey = secretAccessKey,
        region = region,
      )
    val storageEnvironmentVariableProvider =
      StorageEnvironmentVariableProvider(
        buckets = bucketConfig,
        storageConfig = s3StorageConfig,
      )
    val envVarMap = storageEnvironmentVariableProvider.toEnvVarMap()
    assertEquals(10, envVarMap.size)
    assertEquals(bucketConfig.log, envVarMap[EnvVar.STORAGE_BUCKET_LOG.name])
    assertEquals(bucketConfig.workloadOutput, envVarMap[EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT.name])
    assertEquals(bucketConfig.activityPayload, envVarMap[EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD.name])
    assertEquals(bucketConfig.state, envVarMap[EnvVar.STORAGE_BUCKET_STATE.name])
    assertEquals(bucketConfig.auditLogging, envVarMap[EnvVar.STORAGE_BUCKET_AUDIT_LOGGING.name])
    assertEquals(bucketConfig.replicationDump, envVarMap[EnvVar.STORAGE_BUCKET_REPLICATION_DUMP.name])
    assertEquals(StorageType.S3.name, envVarMap[EnvVar.STORAGE_TYPE.name])
    assertEquals(accessKey, envVarMap[EnvVar.AWS_ACCESS_KEY_ID.name])
    assertEquals(secretAccessKey, envVarMap[EnvVar.AWS_SECRET_ACCESS_KEY.name])
    assertEquals(region, envVarMap[EnvVar.AWS_DEFAULT_REGION.name])
  }

  @Test
  internal fun testToEnvVarMapBlankCredentials() {
    val region = Region.US_EAST_1.toString()
    val bucketConfig =
      AirbyteStorageConfig.AirbyteStorageBucketConfig(
        state = "state",
        workloadOutput = "workload-output",
        log = "log",
        activityPayload = "activity-payload",
        auditLogging = "audit-logging",
        profilerOutput = "",
        replicationDump = "",
      )
    val s3StorageConfig =
      AirbyteStorageConfig.S3StorageConfig(
        accessKey = "",
        secretAccessKey = "",
        region = region,
      )
    val storageEnvironmentVariableProvider =
      StorageEnvironmentVariableProvider(
        buckets = bucketConfig,
        storageConfig = s3StorageConfig,
      )
    val envVarMap = storageEnvironmentVariableProvider.toEnvVarMap()
    assertEquals(10, envVarMap.size)
    assertEquals(bucketConfig.log, envVarMap[EnvVar.STORAGE_BUCKET_LOG.name])
    assertEquals(bucketConfig.workloadOutput, envVarMap[EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT.name])
    assertEquals(bucketConfig.activityPayload, envVarMap[EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD.name])
    assertEquals(bucketConfig.state, envVarMap[EnvVar.STORAGE_BUCKET_STATE.name])
    assertEquals(bucketConfig.auditLogging, envVarMap[EnvVar.STORAGE_BUCKET_AUDIT_LOGGING.name])
    assertEquals(bucketConfig.replicationDump, envVarMap[EnvVar.STORAGE_BUCKET_REPLICATION_DUMP.name])
    assertEquals(StorageType.S3.name, envVarMap[EnvVar.STORAGE_TYPE.name])
    assertEquals("", envVarMap[EnvVar.AWS_ACCESS_KEY_ID.name])
    assertEquals("", envVarMap[EnvVar.AWS_SECRET_ACCESS_KEY.name])
    assertEquals(region, envVarMap[EnvVar.AWS_DEFAULT_REGION.name])
  }

  @Test
  internal fun testToString() {
    val accessKey = "access-key"
    val secretAccessKey = "secret-access-key"
    val region = Region.US_EAST_1.toString()
    val s3StorageConfig =
      AirbyteStorageConfig.S3StorageConfig(
        accessKey = accessKey,
        secretAccessKey = secretAccessKey,
        region = region,
      )
    val toString = s3StorageConfig.toString()
    assertEquals("S3StorageConfig(accessKey=*******, secretAccessKey=*******, region=$region)", toString)
  }
}
