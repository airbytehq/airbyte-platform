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
 * Thrown when user sends an invalid input.
 */
class UnknownValueProblem(value: String?) : AbstractThrowableProblem(
  TYPE,
  TITLE,
  HttpStatusType(HttpStatus.BAD_REQUEST),
  String.format("Submitted value could not be found: %s", value),
) {
  companion object {
    @Serial
    private val serialVersionUID = 1L
    private val TYPE = URI.create("$API_DOC_URL/reference/errors")
    private const val TITLE = "value-not-found"
  }

  override fun getCause(): Exceptional? {
    return null
  }
}
