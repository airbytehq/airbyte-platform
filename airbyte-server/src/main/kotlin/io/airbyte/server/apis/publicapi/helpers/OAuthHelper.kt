/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.helpers

import io.airbyte.api.problems.model.generated.ProblemRedirectURLData
import io.airbyte.api.problems.throwable.generated.InvalidRedirectUrlProblem
import io.github.oshai.kotlinlogging.KotlinLogging
import java.net.URI

private val log = KotlinLogging.logger {}

/**
 * Helper for OAuth.
 */
object OAuthHelper {
  private const val TEMP_OAUTH_STATE_KEY = "temp_oauth_state"
  private const val HTTPS = "https"

  fun buildTempOAuthStateKey(state: String): String = "$TEMP_OAUTH_STATE_KEY.$state"

  /**
   * Helper function to validate that a redirect URL is valid and if not, return the appropriate
   * problem.
   */
  fun validateRedirectUrl(redirectUrl: String?) {
    if (redirectUrl == null) throw InvalidRedirectUrlProblem()
    try {
      val uri = URI.create(redirectUrl)
      if (uri.scheme != HTTPS) {
        throw InvalidRedirectUrlProblem(ProblemRedirectURLData().redirectUrl(redirectUrl))
      }
    } catch (e: IllegalArgumentException) {
      log.error(e) { e.message }
      throw InvalidRedirectUrlProblem(ProblemRedirectURLData().redirectUrl(redirectUrl))
    }
  }
}
