/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors

import jakarta.ws.rs.core.Response

/**
 * Thrown when there are authentication issues.
 */
class AuthException : KnownException {
  constructor(message: String?, exception: Exception?) : super(message, exception)

  constructor(message: String?) : super(message)

  override fun getHttpCode(): Int = Response.Status.UNAUTHORIZED.statusCode

  companion object {
    private const val serialVersionUID = 1L
  }
}
