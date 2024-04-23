package io.airbyte.config.storage

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow

val buckets = StorageBucketConfig(log = "log", state = "state", workloadOutput = "workload", activityPayload = "payload")
val config = MinioStorageConfig(buckets = buckets, accessKey = "access", secretAccessKey = "secret", endpoint = "http://endpoint.test")

class MinioS3ClientFactoryTest {
  @Test
  fun `minio doesn't throw exception`() {
    assertDoesNotThrow { MinioS3ClientFactory(config).get() }
  }
}
