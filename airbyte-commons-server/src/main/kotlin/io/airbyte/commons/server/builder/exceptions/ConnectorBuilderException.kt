/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.builder.exceptions

import io.airbyte.commons.server.errors.KnownException
import io.micronaut.http.HttpStatus

/**
 * Thrown when the Connector Builder encountered an error when processing the request.
 */
class ConnectorBuilderException : KnownException {
  constructor(message: String, exception: Exception) : super(message, exception)

  constructor(message: String) : super(message)

  override fun getHttpCode(): Int = HttpStatus.INTERNAL_SERVER_ERROR.code
}
