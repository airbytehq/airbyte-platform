/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.oauth.AUTH_CODE_KEY
import io.airbyte.oauth.BaseOAuth2Flow
import io.airbyte.oauth.CLIENT_ID_KEY
import io.airbyte.oauth.CLIENT_SECRET_KEY
import io.airbyte.oauth.GRANT_TYPE_KEY
import io.airbyte.oauth.REDIRECT_URI_KEY
import io.airbyte.oauth.RESPONSE_TYPE_KEY
import io.airbyte.oauth.SCOPE_KEY
import io.airbyte.oauth.STATE_KEY
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
        .addParameter(CLIENT_ID_KEY, clientId)
        .addParameter(SCOPE_KEY, "advertising::campaign_management")
        .addParameter(RESPONSE_TYPE_KEY, AUTH_CODE_KEY)
        .addParameter(REDIRECT_URI_KEY, redirectUrl)
        .addParameter(STATE_KEY, getState())
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
    mapOf(
      CLIENT_ID_KEY to clientId,
      REDIRECT_URI_KEY to redirectUrl,
      CLIENT_SECRET_KEY to clientSecret,
      AUTH_CODE_KEY to authCode,
      GRANT_TYPE_KEY to "authorization_code",
    )

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
      mapOf(
        "NA" to AUTHORIZE_URL,
        "EU" to AUTHORIZE_EU_URL,
        "FE" to AUTHORIZE_FE_URL,
      )
  }
}
