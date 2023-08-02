/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.problems

import io.airbyte.api.server.constants.API_DOC_URL
import io.micronaut.http.HttpStatus
import io.micronaut.problem.HttpStatusType
import org.zalando.problem.AbstractThrowableProblem
import org.zalando.problem.Exceptional
import java.io.Serial
import java.net.URI

/**
 * Bad request problem for generic 400 errors.
 */
class BadRequestProblem(message: String?) : AbstractThrowableProblem(
  TYPE,
  TITLE,
  HttpStatusType(HttpStatus.BAD_REQUEST),
  message,
) {
  companion object {
    @Serial
    private val serialVersionUID = 1L
    private val TYPE = URI.create("$API_DOC_URL/reference/errors#bad-request")
    private const val TITLE = "bad-request"
  }

  override fun getCause(): Exceptional? {
    return null
  }
}
