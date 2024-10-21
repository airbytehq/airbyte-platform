/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.storage

import io.airbyte.commons.envvar.EnvVar
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class LocalStorageConfigTest {
  @Test
  internal fun testToEnvVarMap() {
    val root = "/root/path"
    val bucketConfig =
      StorageBucketConfig(
        state = "state",
        workloadOutput = "workload-output",
        log = "log",
        activityPayload = "activity-payload",
      )
    val localStorageConfig =
      LocalStorageConfig(
        buckets = bucketConfig,
        root = root,
      )
    val envVarMap = localStorageConfig.toEnvVarMap()
    assertEquals(6, envVarMap.size)
    assertEquals(bucketConfig.log, envVarMap[EnvVar.STORAGE_BUCKET_LOG.name])
    assertEquals(bucketConfig.workloadOutput, envVarMap[EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT.name])
    assertEquals(bucketConfig.activityPayload, envVarMap[EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD.name])
    assertEquals(bucketConfig.state, envVarMap[EnvVar.STORAGE_BUCKET_STATE.name])
    assertEquals(StorageType.LOCAL.name, envVarMap[EnvVar.STORAGE_TYPE.name])
    assertEquals(root, envVarMap[EnvVar.LOCAL_ROOT.name])
  }
}
