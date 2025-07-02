/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.exceptions

import io.airbyte.commons.server.errors.KnownException
import io.micronaut.http.HttpStatus

/**
 * Thrown when the server understands the content type of the request entity, and the syntax of the
 * request entity is correct, but it was unable to process the contained instructions.
 */
class UnprocessableEntityException : KnownException {
  constructor(msg: String) : super(msg)

  constructor(msg: String, t: Throwable) : super(msg, t)

  override fun getHttpCode(): Int = HttpStatus.UNPROCESSABLE_ENTITY.code
}
