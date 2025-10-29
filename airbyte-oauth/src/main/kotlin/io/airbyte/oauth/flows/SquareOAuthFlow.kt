/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.oauth.AUTH_CODE_KEY
import io.airbyte.oauth.BaseOAuth2Flow
import io.airbyte.oauth.CLIENT_ID_KEY
import io.airbyte.oauth.CLIENT_SECRET_KEY
import io.airbyte.oauth.GRANT_TYPE_KEY
import io.airbyte.oauth.SCOPE_KEY
import org.apache.http.client.utils.URIBuilder
import java.io.IOException
import java.net.URISyntaxException
import java.net.URLDecoder
import java.net.http.HttpClient
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.function.Supplier

/**
 * Square OAuth.
 */
class SquareOAuthFlow : BaseOAuth2Flow {
  constructor(httpClient: HttpClient) : super(httpClient)

  @InternalForTesting
  constructor(
    httpClient: HttpClient,
    stateSupplier: Supplier<String>,
  ) : super(httpClient, stateSupplier)

  override fun formatConsentUrl(
    definitionId: UUID?,
    clientId: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
  ): String {
    try {
      // Need to have decoded format, otherwise square fails saying that scope is incorrect
      return URLDecoder.decode(
        URIBuilder(AUTHORIZE_URL)
          .addParameter(CLIENT_ID_KEY, clientId)
          .addParameter(SCOPE_KEY, SCOPES.joinToString("+"))
          .addParameter("session", "False")
          .addParameter("state", getState())
          .build()
          .toString(),
        StandardCharsets.UTF_8,
      )
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
  ): Map<String, String> {
    val scopes = SCOPES.joinToString(separator = ",", prefix = "[", postfix = "]") { name: String -> "\"$name\"" }

    return mapOf(
      CLIENT_ID_KEY to clientId,
      CLIENT_SECRET_KEY to clientSecret,
      AUTH_CODE_KEY to authCode,
      GRANT_TYPE_KEY to "authorization_code",
      io.airbyte.oauth.SCOPES_KEY to scopes,
    )
  }

  companion object {
    private val SCOPES: List<String> =
      listOf(
        "CUSTOMERS_READ",
        "EMPLOYEES_READ",
        "ITEMS_READ",
        "MERCHANT_PROFILE_READ",
        "ORDERS_READ",
        "PAYMENTS_READ",
        "TIMECARDS_READ", // OAuth Permissions:
        // https://developer.squareup.com/docs/oauth-api/square-permissions
        // https://developer.squareup.com/reference/square/enums/OAuthPermission
        // "DISPUTES_READ",
        // "GIFTCARDS_READ",
        // "INVENTORY_READ",
        // "INVOICES_READ",
        // "TIMECARDS_SETTINGS_READ",
        // "LOYALTY_READ",
        // "ONLINE_STORE_SITE_READ",
        // "ONLINE_STORE_SNIPPETS_READ",
        // "SUBSCRIPTIONS_READ",
      )
    private const val AUTHORIZE_URL = "https://connect.squareup.com/oauth2/authorize"
    private const val ACCESS_TOKEN_URL = "https://connect.squareup.com/oauth2/token"
  }
}
