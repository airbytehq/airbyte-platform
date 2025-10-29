/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.oauth.BaseOAuth2Flow
import io.airbyte.oauth.CLIENT_ID_KEY
import io.airbyte.oauth.REDIRECT_URI_KEY
import io.airbyte.oauth.SCOPE_KEY
import org.apache.http.client.utils.URIBuilder
import java.io.IOException
import java.net.URISyntaxException
import java.net.http.HttpClient
import java.util.UUID
import java.util.function.Supplier

/**
 * Slack OAuth.
 */
class SlackOAuthFlow : BaseOAuth2Flow {
  constructor(httpClient: HttpClient) : super(httpClient)

  @InternalForTesting
  constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier)

  /**
   * Depending on the OAuth flow implementation, the URL to grant user's consent may differ,
   * especially in the query parameters to be provided. This function should generate such consent URL
   * accordingly.
   */
  override fun formatConsentUrl(
    definitionId: UUID?,
    clientId: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
  ): String {
    try {
      return URIBuilder(AUTHORIZE_URL)
        .addParameter(CLIENT_ID_KEY, clientId)
        .addParameter(REDIRECT_URI_KEY, redirectUrl)
        .addParameter("state", getState())
        .addParameter(SCOPE_KEY, scopes)
        .build()
        .toString()
    } catch (e: URISyntaxException) {
      throw IOException("Failed to format Consent URL for OAuth flow", e)
    }
  }

  /**
   * Returns the URL where to retrieve the access token from.
   */
  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String = ACCESS_TOKEN_URL

  override fun extractOAuthOutput(
    data: JsonNode,
    accessTokenUrl: String,
  ): Map<String, Any> {
    if (data.has("access_token")) {
      return mapOf("access_token" to data["access_token"].asText())
    } else {
      throw IOException("Missing 'access_token' in query params from $ACCESS_TOKEN_URL")
    }
  }

  private val scopes: String
    get() =
      listOf(
        "channels:history",
        "channels:join",
        "channels:read",
        "groups:read",
        "groups:history",
        "users:read",
        "im:history",
        "mpim:history",
        "im:read",
        "mpim:read",
      ).joinToString(",")

  companion object {
    private const val AUTHORIZE_URL = "https://slack.com/oauth/v2/authorize"
    private const val ACCESS_TOKEN_URL = "https://slack.com/api/oauth.v2.access"
  }
}
