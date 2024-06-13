/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.junit.jupiter.api.Test;

class DefaultS3ClientFactoryTest {

  @Test
  void testS3() {
    final var bucket = new StorageBucketConfig("log", "state", "workload", "payload");
    final var config = new S3StorageConfig(bucket, "access-key", "access-key-secret", "us-east-1");

    assertDoesNotThrow(() -> new DefaultS3ClientFactory(config).get());
  }

}
