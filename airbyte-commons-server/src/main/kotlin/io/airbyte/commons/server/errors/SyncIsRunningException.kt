/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors

/**
 * A sync is running for this connection.
 */
class SyncIsRunningException : KnownException {
  constructor(message: String?) : super(message)

  constructor(message: String?, cause: Throwable?) : super(message, cause)

  override fun getHttpCode(): Int = 423
}
