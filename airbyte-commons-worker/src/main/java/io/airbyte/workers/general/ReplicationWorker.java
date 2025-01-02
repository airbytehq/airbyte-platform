/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import io.airbyte.config.ReplicationOutput;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.workers.exception.WorkerException;
import java.nio.file.Path;

/**
 * Replication Worker. Calls the read method and write methods on a source and destination
 * respectively.
 */
public interface ReplicationWorker {

  /**
   * Blocking call to run the worker's workflow. Once this is complete, getStatus should return either
   * COMPLETE, FAILED, or CANCELLED.
   */
  ReplicationOutput run(ReplicationInput inputType, Path jobRoot) throws WorkerException;

  /**
   * Cancels in-progress workers. Although all workers support cancel, in reality only the
   * asynchronous {@link DefaultReplicationWorker}'s cancel is used.
   */
  void cancel();

}
