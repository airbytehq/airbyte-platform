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
import java.util.function.Supplier

/**
 * Amazon Ads OAuth.
 */
class AmazonAdsOAuthFlow : BaseOAuth2Flow {
  constructor(httpClient: HttpClient) : super(httpClient)

  constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier)

  /**
   * Depending on the OAuth flow implementation, the URL to grant user's consent may differ,
   * especially in the query parameters to be provided. This function should generate such consent URL
   * accordingly.
   *
   * @param definitionId The configured definition ID of this client
   * @param clientId The configured client ID
   * @param redirectUrl the redirect URL
   */
  @Throws(IOException::class)
  override fun formatConsentUrl(
    definitionId: UUID?,
    clientId: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
  ): String {
    try {
      val authorizeUrl =
        if (inputOAuthConfiguration.has("region")) {
          AUTHORIZE_URL_MAP[inputOAuthConfiguration["region"].asText()]
        } else {
          AUTHORIZE_URL
        }
      return URIBuilder(authorizeUrl)
        .addParameter("client_id", clientId)
        .addParameter("scope", "advertising::campaign_management")
        .addParameter("response_type", "code")
        .addParameter("redirect_uri", redirectUrl)
        .addParameter("state", getState())
        .build()
        .toString()
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
      .put("client_id", clientId)
      .put("redirect_uri", redirectUrl)
      .put("client_secret", clientSecret)
      .put("code", authCode)
      .put("grant_type", "authorization_code")
      .build()

  /**
   * Returns the URL where to retrieve the access token from.
   *
   */
  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String = ACCESS_TOKEN_URL

  companion object {
    private const val AUTHORIZE_URL = "https://www.amazon.com/ap/oa"
    private const val AUTHORIZE_EU_URL = "https://eu.account.amazon.com/ap/oa"
    private const val AUTHORIZE_FE_URL = "https://apac.account.amazon.com/ap/oa"
    private const val ACCESS_TOKEN_URL = "https://api.amazon.com/auth/o2/token"

    private val AUTHORIZE_URL_MAP: Map<String, String> =
      ImmutableMap.of(
        "NA",
        AUTHORIZE_URL,
        "EU",
        AUTHORIZE_EU_URL,
        "FE",
        AUTHORIZE_FE_URL,
      )
  }
}
