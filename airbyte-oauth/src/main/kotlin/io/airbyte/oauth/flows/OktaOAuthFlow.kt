/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.collect.ImmutableMap
import io.airbyte.commons.json.Jsons
import io.airbyte.oauth.BaseOAuth2Flow
import org.apache.http.client.utils.URIBuilder
import java.io.IOException
import java.net.URI
import java.net.URISyntaxException
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.UUID

/**
 * Following docs from https://developer.okta.com/docs/guides/implement-oauth-for-okta/main/.
 */
class OktaOAuthFlow(
  httpClient: HttpClient,
) : BaseOAuth2Flow(httpClient) {
  @Throws(IOException::class)
  override fun formatConsentUrl(
    definitionId: UUID?,
    clientId: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
  ): String {
    // getting domain value from user's config

    val domain = getConfigValueUnsafe(inputOAuthConfiguration, "domain")

    val builder =
      URIBuilder()
        .setScheme("https")
        .setHost("$domain.okta.com")
        .setPath("oauth2/v1/authorize") // required
        .addParameter("client_id", clientId)
        .addParameter("redirect_uri", redirectUrl)
        .addParameter("scope", "okta.users.read okta.logs.read okta.groups.read okta.roles.read offline_access")
        .addParameter("response_type", "code")
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
      .put("code", authCode)
      .put("redirect_uri", redirectUrl)
      .put("grant_type", "authorization_code")
      .build()

  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String {
    // getting domain value from user's config
    val domain = getConfigValueUnsafe(inputOAuthConfiguration, "domain")
    return "https://$domain.okta.com/oauth2/v1/token"
  }

  @Throws(IOException::class)
  override fun completeOAuthFlow(
    clientId: String,
    clientSecret: String,
    authCode: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
    oauthParamConfig: JsonNode,
    state: String?,
  ): Map<String, Any> {
    val accessTokenUrl = getAccessTokenUrl(inputOAuthConfiguration)
    val authorization =
      Base64
        .getEncoder()
        .encode(("$clientId:$clientSecret").toByteArray(StandardCharsets.UTF_8))
    val request =
      HttpRequest
        .newBuilder()
        .POST(
          HttpRequest.BodyPublishers
            .ofString(
              tokenReqContentType.converter.apply(
                getAccessTokenQueryParameters(clientId, clientSecret, authCode, redirectUrl),
              ),
            ),
        ).uri(URI.create(accessTokenUrl))
        .header("Content-Type", tokenReqContentType.contentType)
        .header("Accept", "application/json")
        .header("Authorization", "Basic " + String(authorization, StandardCharsets.UTF_8))
        .build()
    try {
      val response =
        httpClient.send(
          request,
          HttpResponse.BodyHandlers.ofString(),
        )

      return extractOAuthOutput(Jsons.deserialize(response.body()), accessTokenUrl)
    } catch (e: InterruptedException) {
      throw IOException("Failed to complete OAuth flow", e)
    }
  }
}
