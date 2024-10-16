/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.storage

import io.airbyte.commons.envvar.EnvVar
import io.airbyte.commons.resources.MoreResources
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class GcsStorageConfigTest {
  @Test
  internal fun testToEnvVarMap() {
    val bucketConfig =
      StorageBucketConfig(
        state = "state",
        workloadOutput = "workload-output",
        log = "log",
        activityPayload = "activity-payload",
      )
    val applicationCredentials = MoreResources.readResource("sample_gcs_credentials.json")
    val gcsStorageConfig =
      GcsStorageConfig(
        buckets = bucketConfig,
        applicationCredentials = applicationCredentials,
      )
    val envVarMap = gcsStorageConfig.toEnvVarMap()
    assertEquals(6, envVarMap.size)
    assertEquals(bucketConfig.log, envVarMap[EnvVar.STORAGE_BUCKET_LOG.name])
    assertEquals(bucketConfig.workloadOutput, envVarMap[EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT.name])
    assertEquals(bucketConfig.activityPayload, envVarMap[EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD.name])
    assertEquals(bucketConfig.state, envVarMap[EnvVar.STORAGE_BUCKET_STATE.name])
    assertEquals(StorageType.GCS.name, envVarMap[EnvVar.STORAGE_TYPE.name])
    assertEquals(applicationCredentials, envVarMap[EnvVar.GOOGLE_APPLICATION_CREDENTIALS.name])
  }

  @Test
  internal fun testToString() {
    val bucketConfig =
      StorageBucketConfig(
        state = "state",
        workloadOutput = "workload-output",
        log = "log",
        activityPayload = "activity-payload",
      )
    val applicationCredentials = MoreResources.readResource("sample_gcs_credentials.json")
    val gcsStorageConfig =
      GcsStorageConfig(
        buckets = bucketConfig,
        applicationCredentials = applicationCredentials,
      )
    val toString = gcsStorageConfig.toString()
    assertEquals("GcsStorageConfig(applicationCredentials=*******)", toString)
  }
}
