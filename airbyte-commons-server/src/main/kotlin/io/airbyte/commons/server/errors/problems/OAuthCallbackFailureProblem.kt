package io.airbyte.commons.server.errors.problems

import io.airbyte.commons.server.API_DOC_URL
import io.micronaut.http.HttpStatus
import java.io.Serial
import java.net.URI

class OAuthCallbackFailureProblem(message: String?) : AbstractThrowableProblem(
  TYPE,
  TITLE,
  HttpStatus.INTERNAL_SERVER_ERROR,
  String.format("Unexpected problem completing OAuth: %s", message),
) {
  companion object {
    @Serial
    private val serialVersionUID = 1L
    private val TYPE = URI.create("$API_DOC_URL/reference/errors#oauth-callback-failure")
    private const val TITLE = "oauth-callback-failure"
  }
}
