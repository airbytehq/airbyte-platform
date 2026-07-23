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
import io.airbyte.oauth.REDIRECT_URI_KEY
import io.airbyte.oauth.SCOPE_KEY
import org.apache.http.client.utils.URIBuilder
import java.io.IOException
import java.net.URISyntaxException
import java.net.http.HttpClient
import java.util.UUID
import java.util.function.Supplier

/**
 * Following docs from https://developer.zendesk.com/documentation/live-chat/getting-started/auth/.
 */
class MondayOAuthFlow : BaseOAuth2Flow {
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
        .setHost("auth.monday.com")
        .setPath("oauth2/authorize") // required
        .addParameter(CLIENT_ID_KEY, clientId)
        .addParameter(REDIRECT_URI_KEY, redirectUrl)
        .addParameter(SCOPE_KEY, "me:read boards:read workspaces:read users:read account:read updates:read assets:read tags:read teams:read")
        .addParameter("state", getState())

    try {
      // applying optional parameter of subdomain, if there is any value
      if (subdomain.isNotEmpty()) {
        builder.addParameter("subdomain", subdomain)
      }
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
      AUTH_CODE_KEY to authCode,
      CLIENT_ID_KEY to clientId,
      CLIENT_SECRET_KEY to clientSecret,
      REDIRECT_URI_KEY to redirectUrl,
    )

  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String = ACCESS_TOKEN_URL

  override fun extractOAuthOutput(
    data: JsonNode,
    accessTokenUrl: String,
  ): Map<String, Any> {
    val result: MutableMap<String, Any> = HashMap()
    // getting out access_token
    if (data.has("access_token")) {
      result["access_token"] = data["access_token"].asText()
    } else {
      throw IOException(String.format("Missing 'access_token' in query params from %s", accessTokenUrl))
    }
    return result
  }

  companion object {
    private const val ACCESS_TOKEN_URL = "https://auth.monday.com/oauth2/token"
  }
}
