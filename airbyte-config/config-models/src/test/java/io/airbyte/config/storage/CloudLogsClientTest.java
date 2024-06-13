/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.storage;

import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class CloudLogsClientTest {

  @Test
  void testGcs() {
    final var bucket = new StorageBucketConfig("log", "state", "workload", "payload");
    final var config = new GcsStorageConfig(bucket, "path/to/google/secret");
    new DefaultGcsClientFactory(config);
  }

}
