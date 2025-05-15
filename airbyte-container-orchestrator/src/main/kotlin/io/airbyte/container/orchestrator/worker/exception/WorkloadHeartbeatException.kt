/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.exception

import io.airbyte.workers.exception.WorkerException
import java.io.Serial

class WorkloadHeartbeatException(
  message: String,
  cause: Throwable? = null,
) : WorkerException(message, cause) {
  companion object {
    @Serial
    private const val serialVersionUID: Long = 1L
  }
}
