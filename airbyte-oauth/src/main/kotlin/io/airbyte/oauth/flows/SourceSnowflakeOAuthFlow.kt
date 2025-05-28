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
 * Snowflake OAuth.
 */
class SourceSnowflakeOAuthFlow : BaseOAuth2Flow {
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
    try {
      val consentUrl =
        URIBuilder(String.format(AUTHORIZE_URL, extractUrl(inputOAuthConfiguration)))
          .addParameter("client_id", clientId)
          .addParameter("redirect_uri", redirectUrl)
          .addParameter("response_type", "code")
          .addParameter("state", getState())
          .build()
          .toString()
      val providedRole = extractRole(inputOAuthConfiguration)
      return if (providedRole.isEmpty()) {
        consentUrl
      } else {
        getConsentUrlWithScopeRole(consentUrl, providedRole)
      }
    } catch (e: URISyntaxException) {
      throw IOException("Failed to format Consent URL for OAuth flow", e)
    }
  }

  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String = String.format(ACCESS_TOKEN_URL, extractUrl(inputOAuthConfiguration))

  @Throws(IOException::class)
  override fun extractCodeParameter(queryParams: Map<String, Any>): String = super.extractCodeParameter(queryParams)

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
      .put("redirect_uri", redirectUrl)
      .build()

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

  @Throws(IOException::class)
  override fun extractOAuthOutput(
    data: JsonNode,
    accessTokenUrl: String,
  ): Map<String, Any> {
    val result: MutableMap<String, Any> = HashMap()
    // access_token is valid for only 10 minutes
    if (data.has("access_token")) {
      result["access_token"] = data["access_token"].asText()
    } else {
      throw IOException(
        String.format(
          "Missing 'access_token' in query params from %s",
          accessTokenUrl,
        ),
      )
    }

    if (data.has("refresh_token")) {
      result["refresh_token"] = data["refresh_token"].asText()
    } else {
      throw IOException(
        String.format(
          "Missing 'refresh_token' in query params from %s",
          accessTokenUrl,
        ),
      )
    }
    if (data.has("username")) {
      result["username"] = data["username"].asText()
    } else {
      throw IOException(
        String.format(
          "Missing 'username' in query params from %s",
          accessTokenUrl,
        ),
      )
    }
    return result
  }

  private fun extractUrl(inputOAuthConfiguration: JsonNode): String {
    val url = inputOAuthConfiguration["host"]
    return if (url == null) "snowflakecomputing.com" else url.asText()
  }

  private fun extractRole(inputOAuthConfiguration: JsonNode): String {
    val role = inputOAuthConfiguration["role"]
    return if (role == null) "" else role.asText()
  }

  companion object {
    private const val AUTHORIZE_URL = "https://%s/oauth/authorize"
    private const val ACCESS_TOKEN_URL = "https://%s/oauth/token-request"

    @Throws(URISyntaxException::class)
    private fun getConsentUrlWithScopeRole(
      consentUrl: String,
      providedRole: String,
    ): String =
      URIBuilder(consentUrl)
        .addParameter("scope", "session:role:$providedRole")
        .build()
        .toString()
  }
}
