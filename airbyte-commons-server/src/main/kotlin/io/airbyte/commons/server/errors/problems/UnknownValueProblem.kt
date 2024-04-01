package io.airbyte.commons.server.errors.problems

import io.airbyte.commons.server.API_DOC_URL
import io.micronaut.http.HttpStatus
import java.io.Serial
import java.net.URI

/**
 * Thrown when user sends an invalid input.
 */
class UnknownValueProblem(value: String?) : AbstractThrowableProblem(
  TYPE,
  TITLE,
  HttpStatus.BAD_REQUEST,
  String.format("Submitted value could not be found: %s", value),
) {
  companion object {
    @Serial
    private val serialVersionUID = 1L
    private val TYPE = URI.create("$API_DOC_URL/reference/errors")
    private const val TITLE = "value-not-found"
  }
}
