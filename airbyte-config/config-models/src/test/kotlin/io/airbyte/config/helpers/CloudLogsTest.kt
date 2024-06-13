package io.airbyte.config.helpers

import io.airbyte.config.storage.GcsStorageConfig
import io.airbyte.config.storage.MinioStorageConfig
import io.airbyte.config.storage.S3StorageConfig
import io.airbyte.config.storage.StorageBucketConfig
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

private val buckets = StorageBucketConfig(log = "log", state = "state", workloadOutput = "workload", activityPayload = "payload")

class CloudLogsTest {
  @Test
  fun `createCloudLogClient for minio`() {
    val storageConfig = MinioStorageConfig(buckets = buckets, accessKey = "ak", secretAccessKey = "sak", endpoint = "e")
    val cfg = LogConfigs(storageConfig)

    assertTrue(CloudLogs.createCloudLogClient(cfg) is S3Logs)
  }

  @Test
  fun `createCloudLogClient for s3`() {
    val storageConfig = S3StorageConfig(buckets = buckets, accessKey = "ak", secretAccessKey = "sak", region = "r")
    val cfg = LogConfigs(storageConfig)

    assertTrue(CloudLogs.createCloudLogClient(cfg) is S3Logs)
  }

  @Test
  fun `createCloudLogClient for gcs`() {
    val storageConfig = GcsStorageConfig(buckets = buckets, applicationCredentials = "ac")
    val cfg = LogConfigs(storageConfig)

    assertTrue(CloudLogs.createCloudLogClient(cfg) is GcsLogs)
  }
}
