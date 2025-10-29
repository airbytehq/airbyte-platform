/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.oauth.AUTH_CODE_KEY
import io.airbyte.oauth.BaseOAuth2Flow
import io.airbyte.oauth.CLIENT_ID_KEY
import io.airbyte.oauth.CLIENT_SECRET_KEY
import io.airbyte.oauth.GRANT_TYPE_KEY
import io.airbyte.oauth.REDIRECT_URI_KEY
import io.airbyte.oauth.REFRESH_TOKEN_KEY
import io.airbyte.oauth.RESPONSE_TYPE_KEY
import org.apache.http.client.utils.URIBuilder
import java.io.IOException
import java.net.URISyntaxException
import java.net.http.HttpClient
import java.util.UUID
import java.util.function.Supplier

/**
 * Following docs from
 * https://devdocs.drift.com/docs/authentication-and-scopes#1-direct-the-user-to-the-drift-oauth-url-.
 */
class DriftOAuthFlow : BaseOAuth2Flow {
  constructor(httpClient: HttpClient) : super(httpClient)

  @InternalForTesting
  internal constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier)

  override fun formatConsentUrl(
    definitionId: UUID?,
    clientId: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
  ): String {
    val builder =
      URIBuilder()
        .setScheme("https")
        .setHost("dev.drift.com")
        .setPath("authorize")
        .addParameter(RESPONSE_TYPE_KEY, AUTH_CODE_KEY)
        .addParameter(CLIENT_ID_KEY, clientId)
        .addParameter(REDIRECT_URI_KEY, redirectUrl)
        .addParameter("state", getState())
    try {
      return builder.build().toString()
    } catch (e: URISyntaxException) {
      throw IOException("Failed to format Consent URL for OAuth flow", e)
    }
  }

  override fun extractCodeParameter(queryParams: Map<String, Any>): String {
    if (queryParams.containsKey(AUTH_CODE_KEY)) {
      return queryParams[AUTH_CODE_KEY] as String
    } else {
      throw IOException("Undefined 'code' from consent redirected url.")
    }
  }

  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String = ACCESS_TOKEN_URL

  override fun getAccessTokenQueryParameters(
    clientId: String,
    clientSecret: String,
    authCode: String,
    redirectUrl: String,
  ): Map<String, String> =
    mapOf(
      CLIENT_ID_KEY to clientId,
      CLIENT_SECRET_KEY to clientSecret,
      GRANT_TYPE_KEY to "authorization_code",
    )

  override fun extractOAuthOutput(
    data: JsonNode,
    accessTokenUrl: String,
  ): Map<String, Any> {
    val result: MutableMap<String, Any> = HashMap()
    if (data.has("access_token")) {
      result["access_token"] = data["access_token"].asText()
    } else {
      throw IOException(String.format("Missing 'access_token' in query params from %s", accessTokenUrl))
    }
    if (data.has(REFRESH_TOKEN_KEY)) {
      result[REFRESH_TOKEN_KEY] = data[REFRESH_TOKEN_KEY].asText()
    } else {
      throw IOException(String.format("Missing 'refresh_token' in query params from %s", accessTokenUrl))
    }
    return result
  }

  companion object {
    private const val ACCESS_TOKEN_URL = "https://driftapi.com/oauth2/token"
  }
}
