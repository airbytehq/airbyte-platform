/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.problems

import io.airbyte.commons.server.errors.problems.AbstractThrowableProblem
import io.airbyte.server.apis.publicapi.constants.API_DOC_URL
import io.micronaut.http.HttpStatus
import java.io.Serial
import java.net.URI

/**
 * UnexpectedProblem allows us to pass through the httpStatus from configApi.
 */
class UnexpectedProblem : AbstractThrowableProblem {
  private var statusCode: Int

  constructor(httpStatus: HttpStatus) : super(
    TYPE,
    TITLE,
    httpStatus,
    "An unexpected problem has occurred. If this is an error that needs to be addressed, please submit a pull request or github issue.",
  ) {
    statusCode = httpStatus.code
  }

  constructor(httpStatus: HttpStatus, message: String?) : super(
    TYPE,
    TITLE,
    httpStatus,
    message,
  ) {
    statusCode = httpStatus.code
  }

  constructor(title: String?, httpStatus: HttpStatus, message: String?) : super(
    TYPE,
    title,
    httpStatus,
    message,
  ) {
    statusCode = httpStatus.code
  }

  companion object {
    @Serial
    private val serialVersionUID = 1L
    private val TYPE = URI.create("$API_DOC_URL/reference/errors")
    private const val TITLE = "unexpected-problem"
  }
}
