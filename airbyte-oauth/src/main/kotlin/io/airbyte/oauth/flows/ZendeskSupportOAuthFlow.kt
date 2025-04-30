/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.collect.ImmutableMap
import io.airbyte.oauth.BaseOAuth2Flow
import org.apache.http.client.utils.URIBuilder
import java.io.IOException
import java.net.URISyntaxException
import java.net.http.HttpClient
import java.util.UUID

/**
 * Following docs from
 * https://support.zendesk.com/hc/en-us/articles/4408845965210-Using-OAuth-authentication-with-your-application.
 */
class ZendeskSupportOAuthFlow(
  httpClient: HttpClient,
) : BaseOAuth2Flow(httpClient) {
  @Throws(IOException::class)
  override fun formatConsentUrl(
    definitionId: UUID?,
    clientId: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
  ): String {
    // getting subdomain value from user's config

    val subdomain = getConfigValueUnsafe(inputOAuthConfiguration, "subdomain")

    val builder =
      URIBuilder()
        .setScheme("https")
        .setHost("$subdomain.zendesk.com")
        .setPath("oauth/authorizations/new") // required
        .addParameter("client_id", clientId)
        .addParameter("redirect_uri", redirectUrl)
        .addParameter("response_type", "code")
        .addParameter("scope", "read")
        .addParameter("state", getState())

    try {
      return builder.build().toString()
    } catch (e: URISyntaxException) {
      throw IOException("Failed to format Consent URL for OAuth flow", e)
    }
  }

  override fun getAccessTokenQueryParameters(
    clientId: String,
    clientSecret: String,
    authCode: String,
    redirectUrl: String,
  ): Map<String, String> =
    ImmutableMap
      .builder<String, String>() // required
      .put("grant_type", "authorization_code")
      .put("code", authCode)
      .put("client_id", clientId)
      .put("client_secret", clientSecret)
      .put("redirect_uri", redirectUrl)
      .put("scope", "read")
      .build()

  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String {
    // getting subdomain value from user's config
    val subdomain = getConfigValueUnsafe(inputOAuthConfiguration, "subdomain")
    return "https://$subdomain.zendesk.com/oauth/tokens"
  }

  @Throws(IOException::class)
  override fun extractOAuthOutput(
    data: JsonNode,
    accessTokenUrl: String,
  ): Map<String, Any> {
    val result: MutableMap<String, Any> = HashMap()
    // getting out access_token
    if (data.has("access_token")) {
      result["access_token"] = data["access_token"].asText()
    } else {
      throw IOException(String.format("Missing 'access_token' in query params from %s", accessTokenUrl))
    }
    return result
  }
}
