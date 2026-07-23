/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.exception

/**
 * Exceptions thrown from a source.
 */
class SourceException : RuntimeException {
  constructor(message: String?) : super(message)

  constructor(message: String?, cause: Throwable?) : super(message, cause)
}
