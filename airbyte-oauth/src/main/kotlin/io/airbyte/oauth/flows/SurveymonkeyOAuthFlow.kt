/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableMap
import io.airbyte.oauth.BaseOAuth2Flow
import org.apache.http.client.utils.URIBuilder
import java.io.IOException
import java.net.URISyntaxException
import java.net.http.HttpClient
import java.util.UUID
import java.util.function.Supplier

/**
 * https://developer.surveymonkey.com/api/v3/?#authentication.
 */
class SurveymonkeyOAuthFlow : BaseOAuth2Flow {
  constructor(httpClient: HttpClient) : super(httpClient)

  @VisibleForTesting
  internal constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier)

  @Throws(Error::class)
  protected fun getBaseURLByOrigin(inputOAuthConfiguration: JsonNode): String {
    val origin = getConfigValueUnsafe(inputOAuthConfiguration, "origin")
    return if (EUROPE == origin) {
      API_ACCESS_URL_EU
    } else if (CANADA == origin) {
      API_ACCESS_URL_CA
    } else if (USA == origin) {
      API_ACCESS_URL_USA
    } else {
      throw Error("Unknown Origin: $origin")
    }
  }

  @Throws(IOException::class)
  override fun formatConsentUrl(
    definitionId: UUID?,
    clientId: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
  ): String {
    try {
      val baseUrl = getBaseURLByOrigin(inputOAuthConfiguration)
      return URIBuilder(baseUrl)
        .setPath(AUTHORIZE_URL)
        .addParameter("client_id", clientId)
        .addParameter("redirect_uri", redirectUrl)
        .addParameter("response_type", "code")
        .addParameter("state", getState())
        .build()
        .toString()
    } catch (e: URISyntaxException) {
      throw IOException("Failed to format Consent URL for OAuth flow", e)
    }
  }

  public override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String {
    val baseURL = getBaseURLByOrigin(inputOAuthConfiguration)
    return baseURL + ACCESS_TOKEN_URL
  }

  override fun getAccessTokenQueryParameters(
    clientId: String,
    clientSecret: String,
    authCode: String,
    redirectUrl: String,
  ): Map<String, String> =
    ImmutableMap
      .builder<String, String>()
      .putAll(super.getAccessTokenQueryParameters(clientId, clientSecret, authCode, redirectUrl))
      .put("grant_type", "authorization_code")
      .build()

  override fun extractOAuthOutput(
    data: JsonNode,
    accessTokenUrl: String,
  ): Map<String, Any> {
    Preconditions.checkArgument(data.has("access_token"), "Missing 'access_token' in query params from %s", ACCESS_TOKEN_URL)
    return java.util.Map.of<String, Any>("access_token", data["access_token"].asText())
  }

  override fun getDefaultOAuthOutputPath(): List<String> = listOf()

  companion object {
    /**
     * https://developer.surveymonkey.com/api/v3/#access-url.
     */
    private const val API_ACCESS_URL_USA = "https://api.surveymonkey.com/"
    private const val API_ACCESS_URL_EU = "https://api.eu.surveymonkey.com/"
    private const val API_ACCESS_URL_CA = "https://api.surveymonkey.ca/"

    private const val EUROPE = "Europe"
    private const val CANADA = "Canada"
    private const val USA = "USA"

    private const val AUTHORIZE_URL = "oauth/authorize"
    private const val ACCESS_TOKEN_URL = "oauth/token"
  }
}
