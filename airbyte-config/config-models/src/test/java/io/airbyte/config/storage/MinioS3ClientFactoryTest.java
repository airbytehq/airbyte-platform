/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.junit.jupiter.api.Test;

class MinioS3ClientFactoryTest {

  @Test
  void testMinio() {
    final var bucket = new StorageBucketConfig("log", "state", "workload");
    final var config = new MinioStorageConfig(bucket, "access", "secret", "http://endpoint.test");

    assertDoesNotThrow(() -> new MinioS3ClientFactory(config).get());
  }

}
