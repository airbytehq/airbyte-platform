/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.oauth.BaseOAuth2Flow
import io.airbyte.oauth.CLIENT_ID_KEY
import io.airbyte.oauth.GRANT_TYPE_KEY
import io.airbyte.oauth.REDIRECT_URI_KEY
import org.apache.http.client.utils.URIBuilder
import java.io.IOException
import java.net.URISyntaxException
import java.net.http.HttpClient
import java.util.UUID
import java.util.function.Supplier

/**
 * Following docs from https://pipedrive.readme.io/docs/marketplace-oauth-authorization.
 */
class PipeDriveOAuthFlow : BaseOAuth2Flow {
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
    super.getAccessTokenQueryParameters(clientId, clientSecret, authCode, redirectUrl) +
      mapOf(GRANT_TYPE_KEY to "authorization_code")

  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String = ACCESS_TOKEN_URL

  override fun getDefaultOAuthOutputPath(): List<String> = listOf("authorization")

  companion object {
    private const val AUTHORIZE_URL = "https://oauth.pipedrive.com/oauth/authorize"
    private const val ACCESS_TOKEN_URL = "https://oauth.pipedrive.com/oauth/token"
  }
}
