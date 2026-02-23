/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
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
  private val WEBAPP_REDIRECT_URL_ALLOWLIST =
    setOf(
      "https://staging-app.airbyte.ai/auth_flow",
      "https://dev-1-cloud.airbyte.com/auth_flow",
      "https://airbyte.cloudns.nz/auth_flow",
      "https://dev-cloud.airbyte.com/auth_flow",
      "https://api.airbyte.com/v1/oauth/callback",
      "https://api.airbyte.ai/api/v1/integrations/connectors/oauth/callback",
      "http://localhost:3000/auth_flow",
      "http://localhost:8000/api/v1/integrations/connectors/oauth/callback",
      "https://cloud.airbyte.com/auth_flow",
      "https://dev-2-cloud.airbyte.com/auth_flow",
      "https://app.airbyte.ai/auth_flow",
      "https://localhost:3000/auth_flow",
      "https://prod-cloud.airbyte.com/auth_flow",
      "https://local.airbyte.dev/auth_flow",
      "http://localhost:8000/auth_flow",
    )

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

  /**
   * Validates the redirect URL for webapp mode using strict exact match allowlisting.
   */
  fun validateWebappRedirectUrlAllowlisted(redirectUrl: String?) {
    if (redirectUrl == null || !WEBAPP_REDIRECT_URL_ALLOWLIST.contains(redirectUrl)) {
      throw InvalidRedirectUrlProblem(ProblemRedirectURLData().redirectUrl(redirectUrl))
    }
  }
}
