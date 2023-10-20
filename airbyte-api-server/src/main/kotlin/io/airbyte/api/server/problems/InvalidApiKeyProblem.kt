/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.problems

import io.airbyte.api.server.constants.API_DOC_URL
import org.zalando.problem.AbstractThrowableProblem
import org.zalando.problem.Exceptional
import java.io.Serial
import java.net.URI

/**
 * Thrown when API key in Authorization header is invalid.
 */
class InvalidApiKeyProblem : AbstractThrowableProblem() {
  @Serial
  private val serialVersionUID = 1L
  private val type = URI.create("$API_DOC_URL/reference/errors#invalid-api-key")
  private val title = "invalid-api-key"

  override fun getCause(): Exceptional? {
    return null
  }
}
