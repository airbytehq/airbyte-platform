/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.logging

import io.airbyte.micronaut.runtime.AirbyteStorageConfig
import io.airbyte.micronaut.runtime.StorageType
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.Environment
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class WorkloadApiStorageConfigTest {
  @Test
  fun `loads managed log storage from deployment variables`() {
    ApplicationContext
      .builder()
      .environments(Environment.TEST)
      .properties(
        mapOf(
          "STORAGE_TYPE" to "GCS",
          "STORAGE_BUCKET_LOG" to "managed-log-bucket",
        ),
      ).start()
      .use { context ->
        val storageConfig = context.getBean(AirbyteStorageConfig::class.java)

        assertEquals(StorageType.GCS, storageConfig.type)
        assertEquals("managed-log-bucket", storageConfig.bucket.log)
      }
  }
}
