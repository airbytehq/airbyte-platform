/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
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
import java.util.function.Supplier

/**
 * Following docs from https://developers.pinterest.com/docs/getting-started/authentication.
 */
class PinterestOAuthFlow : BaseOAuth2Flow {
  constructor(httpClient: HttpClient) : super(httpClient)

  @VisibleForTesting
  constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier)

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
        .setHost("pinterest.com")
        .setPath("oauth") // required
        .addParameter("client_id", clientId)
        .addParameter("redirect_uri", redirectUrl)
        .addParameter("response_type", "code")
        .addParameter("scope", "ads:read,boards:read,boards:read_secret,catalogs:read,pins:read,pins:read_secret,user_accounts:read")
        .addParameter("state", getState())

    try {
      return builder.build().toString()
    } catch (e: URISyntaxException) {
      throw IOException("Failed to format Consent URL for OAuth flow", e)
    }
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
        .encodeToString(("$clientId:$clientSecret").toByteArray(StandardCharsets.UTF_8))
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
        .header("Authorization", "Basic $authorization")
        .build()

    try {
      val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
      return extractOAuthOutput(Jsons.deserialize(response.body()), accessTokenUrl)
    } catch (e: InterruptedException) {
      throw IOException("Failed to complete Pinterest OAuth flow", e)
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

  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String = ACCESS_TOKEN_URL

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
    // getting out refresh_token
    if (data.has("refresh_token")) {
      result["refresh_token"] = data["refresh_token"].asText()
    } else {
      throw IOException(String.format("Missing 'refresh_token' in query params from %s", accessTokenUrl))
    }
    return result
  }

  companion object {
    private const val ACCESS_TOKEN_URL = "https://api.pinterest.com/v5/oauth/token"
  }
}
