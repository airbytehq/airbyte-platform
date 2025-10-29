/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.json.Jsons
import io.airbyte.oauth.AUTH_CODE_KEY
import io.airbyte.oauth.BaseOAuth2Flow
import io.airbyte.oauth.CLIENT_ID_KEY
import io.airbyte.oauth.GRANT_TYPE_KEY
import io.airbyte.oauth.REDIRECT_URI_KEY
import io.airbyte.oauth.REFRESH_TOKEN_KEY
import io.airbyte.oauth.RESPONSE_TYPE_KEY
import io.airbyte.oauth.SCOPE_KEY
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

  @InternalForTesting
  constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier)

  override fun formatConsentUrl(
    definitionId: UUID?,
    clientId: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
  ): String {
    try {
      val consentUrl =
        URIBuilder(String.format(AUTHORIZE_URL, extractUrl(inputOAuthConfiguration)))
          .addParameter(CLIENT_ID_KEY, clientId)
          .addParameter(REDIRECT_URI_KEY, redirectUrl)
          .addParameter(RESPONSE_TYPE_KEY, AUTH_CODE_KEY)
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

  override fun extractCodeParameter(queryParams: Map<String, Any>): String = super.extractCodeParameter(queryParams)

  override fun getAccessTokenQueryParameters(
    clientId: String,
    clientSecret: String,
    authCode: String,
    redirectUrl: String,
  ): Map<String, String> =
    mapOf(
      GRANT_TYPE_KEY to "authorization_code",
      AUTH_CODE_KEY to authCode,
      REDIRECT_URI_KEY to redirectUrl,
    )

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

  override fun extractOAuthOutput(
    data: JsonNode,
    accessTokenUrl: String,
  ): Map<String, Any> {
    val result: MutableMap<String, Any> = mutableMapOf()
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

    if (data.has(REFRESH_TOKEN_KEY)) {
      result[REFRESH_TOKEN_KEY] = data[REFRESH_TOKEN_KEY].asText()
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

    private fun getConsentUrlWithScopeRole(
      consentUrl: String,
      providedRole: String,
    ): String =
      URIBuilder(consentUrl)
        .addParameter(SCOPE_KEY, "session:role:$providedRole")
        .build()
        .toString()
  }
}
