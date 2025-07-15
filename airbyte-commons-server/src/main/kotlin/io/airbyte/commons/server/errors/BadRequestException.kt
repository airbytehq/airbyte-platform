/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors

/**
 * Simple exception for returning a 400 from an HTTP endpoint.
 */
open class BadRequestException : KnownException {
  constructor(msg: String?) : super(msg)

  constructor(message: String?, cause: Throwable?) : super(message, cause)

  override fun getHttpCode(): Int = 400
}
