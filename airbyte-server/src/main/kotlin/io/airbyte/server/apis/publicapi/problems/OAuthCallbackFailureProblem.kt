/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.server.apis.publicapi.problems

import io.airbyte.commons.server.errors.problems.AbstractThrowableProblem
import io.airbyte.server.apis.publicapi.constants.API_DOC_URL
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
