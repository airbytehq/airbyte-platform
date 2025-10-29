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
import org.apache.http.client.utils.URIBuilder
import java.io.IOException
import java.net.URISyntaxException
import java.net.http.HttpClient
import java.util.UUID
import java.util.function.Supplier

/**
 * Microsoft Bing Ads OAuth.
 */
class MicrosoftBingAdsOAuthFlow : BaseOAuth2Flow {
  constructor(httpClient: HttpClient) : super(httpClient)

  @InternalForTesting
  constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier)

  private val scopes: String
    get() = "offline_access%20https://ads.microsoft.com/msads.manage"

  override fun formatConsentUrl(
    definitionId: UUID?,
    clientId: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
  ): String {
    val tenantId: String
    try {
      tenantId = getConfigValueUnsafe(inputOAuthConfiguration, FIELD_NAME)
    } catch (e: IllegalArgumentException) {
      throw IOException("Failed to get $FIELD_NAME value from input configuration", e)
    }

    try {
      return URIBuilder()
        .setScheme("https")
        .setHost("login.microsoftonline.com")
        .setPath("$tenantId/oauth2/v2.0/authorize")
        .addParameter(CLIENT_ID_KEY, clientId)
        .addParameter(RESPONSE_TYPE_KEY, AUTH_CODE_KEY)
        .addParameter(REDIRECT_URI_KEY, redirectUrl)
        .addParameter("response_mode", "query")
        .addParameter("state", getState())
        .build()
        .toString() + "&scope=" + scopes
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
      GRANT_TYPE_KEY to "authorization_code",
      AUTH_CODE_KEY to authCode,
      CLIENT_ID_KEY to clientId,
      REDIRECT_URI_KEY to redirectUrl,
    )

  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String {
    val tenantId = getConfigValueUnsafe(inputOAuthConfiguration, FIELD_NAME)
    return "https://login.microsoftonline.com/$tenantId/oauth2/v2.0/token"
  }

  companion object {
    private const val FIELD_NAME = "tenant_id"
  }
}
