/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.oauth.AUTH_CODE_KEY
import io.airbyte.oauth.BaseOAuth2Flow
import io.airbyte.oauth.CLIENT_ID_KEY
import io.airbyte.oauth.CLIENT_SECRET_KEY
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
 * Following docs from
 * https://developer.zendesk.com/api-reference/custom-data/introduction/#authentication.
 */
class ZendeskSunshineOAuthFlow : BaseOAuth2Flow {
  constructor(httpClient: HttpClient) : super(httpClient)

  @InternalForTesting
  constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier)

  override fun formatConsentUrl(
    definitionId: UUID?,
    clientId: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
  ): String {
    // getting subdomain value from user's config

    val subdomain = getConfigValueUnsafe(inputOAuthConfiguration, "subdomain")

    val builder =
      URIBuilder()
        .setScheme("https")
        .setHost(String.format("%s.zendesk.com", subdomain))
        .setPath("oauth/authorizations/new") // required
        .addParameter(RESPONSE_TYPE_KEY, AUTH_CODE_KEY)
        .addParameter(REDIRECT_URI_KEY, redirectUrl)
        .addParameter(CLIENT_ID_KEY, clientId)
        .addParameter(SCOPE_KEY, "read")
        .addParameter("state", getState())

    try {
      return builder.build().toString()
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
      CLIENT_SECRET_KEY to clientSecret,
      REDIRECT_URI_KEY to redirectUrl,
      SCOPE_KEY to "read",
    )

  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String {
    // getting subdomain value from user's config
    val subdomain = getConfigValueUnsafe(inputOAuthConfiguration, "subdomain")

    return String.format("https://%s.zendesk.com/oauth/tokens", subdomain)
  }

  override fun extractOAuthOutput(
    data: JsonNode,
    accessTokenUrl: String,
  ): Map<String, Any> {
    val result: MutableMap<String, Any> = mutableMapOf()
    // getting out access_token
    if (data.has("access_token")) {
      result["access_token"] = data["access_token"].asText()
    } else {
      throw IOException("Missing 'access_token' in query params from $accessTokenUrl")
    }
    return result
  }
}
