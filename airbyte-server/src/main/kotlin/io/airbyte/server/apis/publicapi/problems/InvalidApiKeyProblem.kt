/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.problems

import io.airbyte.commons.server.errors.problems.AbstractThrowableProblem
import io.airbyte.server.apis.publicapi.constants.API_DOC_URL
import io.micronaut.http.HttpStatus
import java.io.Serial
import java.net.URI

/**
 * Thrown when API key in Authorization header is invalid.
 */
class InvalidApiKeyProblem : AbstractThrowableProblem(
  type,
  title,
  HttpStatus.UNAUTHORIZED,
  "The API key in the Authorization header is invalid.",
) {
  companion object {
    @Serial
    private val serialVersionUID = 1L
    private val type = URI.create("$API_DOC_URL/reference/errors#invalid-api-key")
    private val title = "invalid-api-key"
  }
}
