package io.airbyte.commons.server.errors.problems

import io.airbyte.commons.server.API_DOC_URL
import io.micronaut.http.HttpStatus
import java.io.Serial
import java.net.URI

/**
 * Problem to indicate an invalid URL format.
 */
class InvalidRedirectUrlProblem(url: String?) : AbstractThrowableProblem(
  TYPE,
  TITLE,
  HttpStatus.UNPROCESSABLE_ENTITY,
  String.format("Redirect URL format not understood: %s", url),
) {
  companion object {
    @Serial
    private val serialVersionUID = 1L
    private val TYPE = URI.create("$API_DOC_URL/reference/errors#invalid-redirect-url")
    private const val TITLE = "invalid-redirect-url"
  }
}
