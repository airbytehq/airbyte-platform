/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.oauth.AUTH_CODE_KEY
import io.airbyte.oauth.BaseOAuth2Flow
import io.airbyte.oauth.CLIENT_ID_KEY
import io.airbyte.oauth.CLIENT_SECRET_KEY
import io.airbyte.oauth.GRANT_TYPE_KEY
import io.airbyte.oauth.REDIRECT_URI_KEY
import io.airbyte.oauth.RESPONSE_TYPE_KEY
import io.airbyte.oauth.SCOPE_KEY
import org.apache.http.client.utils.URIBuilder
import java.io.IOException
import java.net.URISyntaxException
import java.net.URLDecoder
import java.net.http.HttpClient
import java.nio.charset.StandardCharsets
import java.util.UUID

/**
 * Lever OAuth.
 */
class LeverOAuthFlow(
  httpClient: HttpClient,
) : BaseOAuth2Flow(httpClient) {
  private fun getAudience(inputOAuthConfiguration: JsonNode): String = String.format("%s/v1/", getBaseApiUrl(inputOAuthConfiguration))

  override fun getAccessTokenQueryParameters(
    clientId: String,
    clientSecret: String,
    authCode: String,
    redirectUrl: String,
  ): Map<String, String> =
    mapOf(
      CLIENT_ID_KEY to clientId,
      REDIRECT_URI_KEY to redirectUrl,
      CLIENT_SECRET_KEY to clientSecret,
      GRANT_TYPE_KEY to "authorization_code",
      AUTH_CODE_KEY to authCode,
    )

  /**
   * Returns the URL where to retrieve the access token from.
   */
  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String = String.format(ACCESS_TOKEN_URL, getBaseAuthUrl(inputOAuthConfiguration))

  override fun formatConsentUrl(
    definitionId: UUID?,
    clientId: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
  ): String {
    try {
      return URLDecoder.decode(
        (
          URIBuilder(String.format(AUTHORIZE_URL, getBaseAuthUrl(inputOAuthConfiguration)))
            .addParameter(CLIENT_ID_KEY, clientId)
            .addParameter(REDIRECT_URI_KEY, redirectUrl)
            .addParameter("state", getState())
            .addParameter(RESPONSE_TYPE_KEY, AUTH_CODE_KEY)
            .addParameter(SCOPE_KEY, SCOPES)
            .addParameter("audience", getAudience(inputOAuthConfiguration))
            .addParameter("prompt", "consent")
            .build()
            .toString()
        ),
        StandardCharsets.UTF_8,
      )
    } catch (e: URISyntaxException) {
      throw IOException("Failed to format Consent URL for OAuth flow", e)
    }
  }

  private fun getBaseAuthUrl(inputOAuthConfiguration: JsonNode): String =
    if (isProduction(inputOAuthConfiguration)) {
      "http1s://auth.lever.co"
    } else {
      "https://sandbox-lever.auth0.com"
    }

  private fun getBaseApiUrl(inputOAuthConfiguration: JsonNode): String =
    if (isProduction(inputOAuthConfiguration)) {
      "https://api.lever.co/"
    } else {
      "https://api.sandbox.lever.co"
    }

  private fun isProduction(inputOAuthConfiguration: JsonNode): Boolean {
    val environment = inputOAuthConfiguration["environment"]
    return environment != null &&
      "production" == environment.asText().lowercase()
  }

  companion object {
    private const val AUTHORIZE_URL = "%s/authorize"
    private const val ACCESS_TOKEN_URL = "%s/oauth/token"

    private val SCOPES: String =
      listOf(
        "applications:read:admin",
        "applications:read:admin",
        "interviews:read:admin",
        "notes:read:admin",
        "offers:read:admin",
        "opportunities:read:admin",
        "referrals:read:admin",
        "resumes:read:admin",
        "users:read:admin",
        "offline_access",
      ).joinToString("+")
  }
}
