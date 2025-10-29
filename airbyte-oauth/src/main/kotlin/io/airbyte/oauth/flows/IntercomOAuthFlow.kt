/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.oauth.AUTH_CODE_KEY
import io.airbyte.oauth.BaseOAuth2Flow
import io.airbyte.oauth.CLIENT_ID_KEY
import io.airbyte.oauth.REDIRECT_URI_KEY
import io.airbyte.oauth.RESPONSE_TYPE_KEY
import org.apache.http.client.utils.URIBuilder
import java.io.IOException
import java.net.URISyntaxException
import java.net.http.HttpClient
import java.util.UUID
import java.util.function.Supplier

/**
 * Intercom OAuth.
 */
class IntercomOAuthFlow : BaseOAuth2Flow {
  constructor(httpClient: HttpClient) : super(httpClient)

  @InternalForTesting
  constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier)

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
        .addParameter(RESPONSE_TYPE_KEY, AUTH_CODE_KEY)
        .addParameter("state", getState())
        .build()
        .toString()
    } catch (e: URISyntaxException) {
      throw IOException("Failed to format Consent URL for OAuth flow", e)
    }
  }

  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String = ACCESS_TOKEN_URL

  override fun extractOAuthOutput(
    data: JsonNode,
    accessTokenUrl: String,
  ): Map<String, Any> {
    // Intercom does not have refresh token but calls it "long lived access token" instead:
    // see https://developers.intercom.com/building-apps/docs/setting-up-oauth
    require(data.has("access_token")) { "Missing 'access_token' in query params from $ACCESS_TOKEN_URL" }
    return mapOf("access_token" to data["access_token"].asText())
  }

  override fun getDefaultOAuthOutputPath(): List<String> = listOf()

  companion object {
    private const val AUTHORIZE_URL = "https://app.intercom.com/a/oauth/connect"
    private const val ACCESS_TOKEN_URL = "https://api.intercom.io/auth/eagle/token"
  }
}
