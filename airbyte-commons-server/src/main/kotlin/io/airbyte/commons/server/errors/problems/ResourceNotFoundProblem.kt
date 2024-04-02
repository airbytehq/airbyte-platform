package io.airbyte.commons.server.errors.problems

import io.airbyte.commons.server.API_DOC_URL
import io.micronaut.http.HttpStatus
import java.io.Serial
import java.net.URI

/**
 * Thrown when user attempts to interact with a resource that can't be found in the db.
 */
class ResourceNotFoundProblem(resourceId: String?) : AbstractThrowableProblem(
  TYPE,
  TITLE,
  HttpStatus.BAD_REQUEST,
  String.format("Could not find a resource for: %s", resourceId),
) {
  companion object {
    @Serial
    private val serialVersionUID = 1L
    private val TYPE = URI.create("$API_DOC_URL/reference/errors#resource-not-found")
    private const val TITLE = "resource-not-found"
  }
}
