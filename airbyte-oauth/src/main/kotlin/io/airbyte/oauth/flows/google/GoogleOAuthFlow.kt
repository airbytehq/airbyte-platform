/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows.google

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
import com.google.common.collect.ImmutableMap
import io.airbyte.oauth.BaseOAuth2Flow
import org.apache.http.client.utils.URIBuilder
import java.io.IOException
import java.net.URISyntaxException
import java.net.http.HttpClient
import java.util.UUID
import java.util.function.Supplier

/**
 * Google OAuth.
 *
 * Following docs from https://developers.google.com/identity/protocols/oauth2/web-server
 */
abstract class GoogleOAuthFlow : BaseOAuth2Flow {
  constructor(httpClient: HttpClient) : super(httpClient)

  @VisibleForTesting
  internal constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier)

  @Throws(IOException::class)
  override fun formatConsentUrl(
    definitionId: UUID?,
    clientId: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
  ): String {
    val builder =
      URIBuilder()
        .setScheme("https")
        .setHost("accounts.google.com")
        .setPath("o/oauth2/v2/auth")
        .addParameter("client_id", clientId)
        .addParameter("redirect_uri", redirectUrl)
        .addParameter("response_type", "code")
        .addParameter("scope", getScope()) // recommended
        .addParameter("access_type", "offline")
        .addParameter("state", getState()) // optional
        .addParameter("include_granted_scopes", "true") // .addParameter("login_hint", "user_email")
        .addParameter("prompt", "consent")
    try {
      return builder.build().toString()
    } catch (e: URISyntaxException) {
      throw IOException("Failed to format Consent URL for OAuth flow", e)
    }
  }

  /**
   * Get scope.
   *
   * @return the scope for the specific google oauth implementation.
   */
  protected abstract fun getScope(): String

  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String = ACCESS_TOKEN_URL

  override fun getAccessTokenQueryParameters(
    clientId: String,
    clientSecret: String,
    authCode: String,
    redirectUrl: String,
  ): Map<String, String> =
    ImmutableMap
      .builder<String, String>()
      .put("client_id", clientId)
      .put("client_secret", clientSecret)
      .put("code", authCode)
      .put("grant_type", "authorization_code")
      .put("redirect_uri", redirectUrl)
      .build()

  @Throws(IOException::class)
  override fun extractOAuthOutput(
    data: JsonNode,
    accessTokenUrl: String,
  ): Map<String, Any> {
    val result = super.extractOAuthOutput(data, accessTokenUrl).toMutableMap()
    if (data.has("access_token")) {
      // google also returns an access token the first time you complete oauth flow
      result["access_token"] = data["access_token"].asText()
    }
    return result
  }

  companion object {
    private const val ACCESS_TOKEN_URL = "https://oauth2.googleapis.com/token"
  }
}
