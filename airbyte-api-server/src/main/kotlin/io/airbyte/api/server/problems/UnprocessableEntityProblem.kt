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
 * Thrown when request body cannot be processed correctly.
 */
class UnprocessableEntityProblem : AbstractThrowableProblem {
  constructor() : super(
    TYPE,
    TITLE,
    HttpStatusType(HttpStatus.UNPROCESSABLE_ENTITY),
    "The body of the request was not understood",
  )

  constructor(message: String?) : super(
    TYPE,
    TITLE,
    HttpStatusType(HttpStatus.UNPROCESSABLE_ENTITY),
    message,
  )

  companion object {
    @Serial
    private val serialVersionUID = 1L
    private val TYPE = URI.create("$API_DOC_URL/reference/errors#unprocessable-entity")
    private const val TITLE = "unprocessable-entity"
  }

  override fun getCause(): Exceptional? {
    return null
  }
}
