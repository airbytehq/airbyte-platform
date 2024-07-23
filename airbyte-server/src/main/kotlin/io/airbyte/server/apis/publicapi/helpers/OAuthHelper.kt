/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.helpers

import io.airbyte.api.problems.model.generated.ProblemRedirectURLData
import io.airbyte.api.problems.throwable.generated.InvalidRedirectUrlProblem
import org.slf4j.LoggerFactory
import java.net.URI

/**
 * Helper for OAuth.
 */
object OAuthHelper {
  private const val TEMP_OAUTH_STATE_KEY = "temp_oauth_state"
  private const val HTTPS = "https"
  private val log = LoggerFactory.getLogger(OAuthHelper.javaClass)

  fun buildTempOAuthStateKey(state: String): String {
    return "$TEMP_OAUTH_STATE_KEY.$state"
  }

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
      log.error(e.message)
      throw InvalidRedirectUrlProblem(ProblemRedirectURLData().redirectUrl(redirectUrl))
    }
  }
}
