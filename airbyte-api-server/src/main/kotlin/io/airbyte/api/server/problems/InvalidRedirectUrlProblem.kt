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
 * Problem to indicate an invalid URL format.
 */
class InvalidRedirectUrlProblem(url: String?) : AbstractThrowableProblem(
  TYPE,
  TITLE,
  HttpStatusType(HttpStatus.UNPROCESSABLE_ENTITY),
  String.format("Redirect URL format not understood: %s", url),
) {
  companion object {
    @Serial
    private val serialVersionUID = 1L
    private val TYPE = URI.create("$API_DOC_URL/reference/errors#invalid-redirect-url")
    private const val TITLE = "invalid-redirect-url"
  }

  override fun getCause(): Exceptional? {
    return null
  }
}
