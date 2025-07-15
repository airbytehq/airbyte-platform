/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors

import io.micronaut.http.HttpStatus

/**
 * Exception when a request conflicts with the current state of the server. For example, trying to
 * accept an invitation that was already accepted.
 */
class ConflictException : KnownException {
  constructor(message: String?) : super(message)

  constructor(message: String?, cause: Throwable?) : super(message, cause)

  override fun getHttpCode(): Int = HttpStatus.CONFLICT.code
}
