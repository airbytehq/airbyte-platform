/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.exception

class InvalidChecksumException(message: String) : WorkerException(message) {
  companion object {
    @Suppress("ConstPropertyName")
    @java.io.Serial
    private const val serialVersionUID: Long = 1L
  }
}
