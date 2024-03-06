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
 * Bad request problem for generic 400 errors.
 */
class BadRequestProblem(message: String?) : AbstractThrowableProblem(
  TYPE,
  TITLE,
  HttpStatus.BAD_REQUEST,
  message,
) {
  companion object {
    @Serial
    private val serialVersionUID = 1L
    private val TYPE = URI.create("$API_DOC_URL/reference/errors#bad-request")
    private const val TITLE = "bad-request"
  }
}
