/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.storage

import io.airbyte.commons.envvar.EnvVar
import io.airbyte.commons.resources.Resources
import io.airbyte.micronaut.runtime.AirbyteStorageConfig
import io.airbyte.micronaut.runtime.StorageEnvironmentVariableProvider
import io.airbyte.micronaut.runtime.StorageType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class GcsStorageConfigTest {
  @Test
  internal fun testToEnvVarMap() {
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
    val applicationCredentials = Resources.read("sample_gcs_credentials.json")
    val gcsStorageConfig =
      AirbyteStorageConfig.GcsStorageConfig(
        applicationCredentials = applicationCredentials,
      )
    val storageEnvironmentVariableProvider =
      StorageEnvironmentVariableProvider(
        buckets = bucketConfig,
        storageConfig = gcsStorageConfig,
      )
    val envVarMap = storageEnvironmentVariableProvider.toEnvVarMap()
    assertEquals(8, envVarMap.size)
    assertEquals(bucketConfig.log, envVarMap[EnvVar.STORAGE_BUCKET_LOG.name])
    assertEquals(bucketConfig.workloadOutput, envVarMap[EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT.name])
    assertEquals(bucketConfig.activityPayload, envVarMap[EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD.name])
    assertEquals(bucketConfig.state, envVarMap[EnvVar.STORAGE_BUCKET_STATE.name])
    assertEquals(bucketConfig.auditLogging, envVarMap[EnvVar.STORAGE_BUCKET_AUDIT_LOGGING.name])
    assertEquals(bucketConfig.replicationDump, envVarMap[EnvVar.STORAGE_BUCKET_REPLICATION_DUMP.name])
    assertEquals(StorageType.GCS.name, envVarMap[EnvVar.STORAGE_TYPE.name])
    assertEquals(applicationCredentials, envVarMap[EnvVar.GOOGLE_APPLICATION_CREDENTIALS.name])
  }

  @Test
  internal fun testToEnvVarMapWithAuditLogging() {
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
    val applicationCredentials = Resources.read("sample_gcs_credentials.json")
    val gcsStorageConfig =
      AirbyteStorageConfig.GcsStorageConfig(
        applicationCredentials = applicationCredentials,
      )
    val storageEnvironmentVariableProvider =
      StorageEnvironmentVariableProvider(
        buckets = bucketConfig,
        storageConfig = gcsStorageConfig,
      )
    val envVarMap = storageEnvironmentVariableProvider.toEnvVarMap()
    assertEquals(8, envVarMap.size)
    assertEquals(bucketConfig.log, envVarMap[EnvVar.STORAGE_BUCKET_LOG.name])
    assertEquals(bucketConfig.workloadOutput, envVarMap[EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT.name])
    assertEquals(bucketConfig.activityPayload, envVarMap[EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD.name])
    assertEquals(bucketConfig.state, envVarMap[EnvVar.STORAGE_BUCKET_STATE.name])
    assertEquals(bucketConfig.auditLogging, envVarMap[EnvVar.STORAGE_BUCKET_AUDIT_LOGGING.name])
    assertEquals(bucketConfig.replicationDump, envVarMap[EnvVar.STORAGE_BUCKET_REPLICATION_DUMP.name])
    assertEquals(StorageType.GCS.name, envVarMap[EnvVar.STORAGE_TYPE.name])
    assertEquals(applicationCredentials, envVarMap[EnvVar.GOOGLE_APPLICATION_CREDENTIALS.name])
  }

  @Test
  internal fun testToString() {
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
    val applicationCredentials = Resources.read("sample_gcs_credentials.json")
    val gcsStorageConfig =
      AirbyteStorageConfig.GcsStorageConfig(
        applicationCredentials = applicationCredentials,
      )
    val toString = gcsStorageConfig.toString()
    assertEquals("GcsStorageConfig(applicationCredentials=*******)", toString)
  }
}
