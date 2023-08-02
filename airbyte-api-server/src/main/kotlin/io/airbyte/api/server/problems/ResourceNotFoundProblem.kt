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
 * Thrown when user attempts to interact with a resource that can't be found in the db.
 */
class ResourceNotFoundProblem(resourceId: String?) : AbstractThrowableProblem(
  TYPE,
  TITLE,
  HttpStatusType(HttpStatus.BAD_REQUEST),
  String.format("Could not find a resource for: %s", resourceId),
) {
  companion object {
    @Serial
    private val serialVersionUID = 1L
    private val TYPE = URI.create("$API_DOC_URL/reference/errors#resource-not-found")
    private const val TITLE = "resource-not-found"
  }

  override fun getCause(): Exceptional? {
    return null
  }
}
