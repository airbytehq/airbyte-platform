/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
import com.google.common.collect.ImmutableMap
import io.airbyte.oauth.BaseOAuth2Flow
import org.apache.http.client.utils.URIBuilder
import java.io.IOException
import java.net.URISyntaxException
import java.net.URLDecoder
import java.net.http.HttpClient
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.function.Supplier
import java.util.stream.Collectors

/**
 * Square OAuth.
 */
class SquareOAuthFlow : BaseOAuth2Flow {
  constructor(httpClient: HttpClient) : super(httpClient)

  @VisibleForTesting
  constructor(
    httpClient: HttpClient,
    stateSupplier: Supplier<String>,
  ) : super(httpClient, stateSupplier)

  @Throws(IOException::class)
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
          .addParameter("client_id", clientId)
          .addParameter("scope", java.lang.String.join("+", SCOPES))
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
    var scopes =
      SCOPES
        .stream()
        .map { name: String -> ('"'.toString() + name + '"') }
        .collect(Collectors.joining(","))
    scopes = "[$scopes]"

    return ImmutableMap
      .builder<String, String>() // required
      .put("client_id", clientId)
      .put("client_secret", clientSecret)
      .put("code", authCode)
      .put("grant_type", "authorization_code")
      .put("scopes", scopes)
      .build()
  }

  companion object {
    private val SCOPES: List<String> =
      mutableListOf(
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
