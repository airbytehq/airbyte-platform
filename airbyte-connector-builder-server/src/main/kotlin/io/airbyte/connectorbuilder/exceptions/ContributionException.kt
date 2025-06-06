/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.exceptions

import io.airbyte.commons.server.errors.KnownException
import io.micronaut.http.HttpStatus

/**
 * Thrown when the Connector Contribution encountered an error when processing the request.
 */
class ContributionException : KnownException {
  var httpStatus: HttpStatus = HttpStatus.UNAUTHORIZED

  constructor(message: String?, exception: Exception?) : super(message, exception)

  constructor(message: String?) : super(message)

  constructor(message: String?, httpStatus: HttpStatus) : super(message) {
    this.httpStatus = httpStatus
  }

  override fun getHttpCode(): Int = httpStatus.code
}
