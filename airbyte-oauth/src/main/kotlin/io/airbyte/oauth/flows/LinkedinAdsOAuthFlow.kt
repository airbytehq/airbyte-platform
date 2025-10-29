/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.oauth.AUTH_CODE_KEY
import io.airbyte.oauth.BaseOAuth2Flow
import io.airbyte.oauth.CLIENT_ID_KEY
import io.airbyte.oauth.GRANT_TYPE_KEY
import io.airbyte.oauth.REDIRECT_URI_KEY
import io.airbyte.oauth.RESPONSE_TYPE_KEY
import io.airbyte.oauth.SCOPE_KEY
import org.apache.http.client.utils.URIBuilder
import java.io.IOException
import java.net.URISyntaxException
import java.net.http.HttpClient
import java.util.UUID
import java.util.function.Supplier

/**
 * LinkedIn OAuth.
 */
class LinkedinAdsOAuthFlow : BaseOAuth2Flow {
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
      return URIBuilder(AUTHORIZE_URL)
        .addParameter(CLIENT_ID_KEY, clientId)
        .addParameter(REDIRECT_URI_KEY, redirectUrl)
        .addParameter(RESPONSE_TYPE_KEY, AUTH_CODE_KEY)
        .addParameter(SCOPE_KEY, SCOPES)
        .addParameter("state", getState())
        .build()
        .toString()
    } catch (e: URISyntaxException) {
      throw IOException("Failed to format Consent URL for OAuth flow", e)
    }
  }

  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String = ACCESS_TOKEN_URL

  override fun getAccessTokenQueryParameters(
    clientId: String,
    clientSecret: String,
    authCode: String,
    redirectUrl: String,
  ): Map<String, String> =
    super.getAccessTokenQueryParameters(clientId, clientSecret, authCode, redirectUrl) +
      mapOf(GRANT_TYPE_KEY to "authorization_code")

  companion object {
    private const val AUTHORIZE_URL = "https://www.linkedin.com/oauth/v2/authorization"
    private const val ACCESS_TOKEN_URL = "https://www.linkedin.com/oauth/v2/accessToken"
    private const val SCOPES = "r_ads_reporting r_emailaddress r_liteprofile r_ads r_basicprofile r_organization_social"
  }
}
