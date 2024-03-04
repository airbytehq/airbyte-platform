/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

public class ConcurrentQueueBufferedReplicationWorkerTest extends BufferedReplicationWorkerTest {

  @Override
  public BufferedReplicationWorkerType getQueueType() {
    return BufferedReplicationWorkerType.BUFFERED;
  }

}
