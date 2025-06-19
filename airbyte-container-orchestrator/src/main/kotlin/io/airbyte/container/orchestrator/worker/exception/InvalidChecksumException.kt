/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.exception

import io.airbyte.workers.exception.WorkerException
import java.io.Serial

class InvalidChecksumException(
  message: String,
) : WorkerException(message) {
  companion object {
    @Serial
    private const val serialVersionUID: Long = 1L
  }
}
