/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.exception

/**
 * Exceptions thrown from a destination.
 */
class DestinationException : RuntimeException {
  constructor(message: String?) : super(message)

  constructor(message: String?, cause: Throwable?) : super(message, cause)
}
