package io.airbyte.commons.server.errors.problems

import io.airbyte.commons.server.API_DOC_URL
import io.micronaut.http.HttpStatus
import java.io.Serial
import java.net.URI

/**
 * Thrown when request body cannot be processed correctly.
 */
class NoUserFoundProblem : AbstractThrowableProblem {
  constructor() : super(
    TYPE,
    TITLE,
    HttpStatus.NOT_FOUND,
    "No user found.",
  )

  constructor(message: String?) : super(
    TYPE,
    TITLE,
    HttpStatus.UNPROCESSABLE_ENTITY,
    message,
  )

  companion object {
    @Serial
    private val serialVersionUID = 1L
    private val TYPE = URI.create("$API_DOC_URL/reference/errors#500-no-user-found")
    private const val TITLE = "unprocessable-entity"
  }
}
