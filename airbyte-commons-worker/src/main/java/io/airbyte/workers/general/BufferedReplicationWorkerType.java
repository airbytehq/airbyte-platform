/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

public enum BufferedReplicationWorkerType {

  BUFFERED("buffered"),
  BUFFERED_WITH_LINKED_BLOCKING_QUEUE("buffered_with_linked_blocking_queue"),
  ;

  public final String workerType;

  BufferedReplicationWorkerType(final String workerType) {
    this.workerType = workerType;
  }

}
