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
import io.airbyte.oauth.REFRESH_TOKEN_KEY
import io.airbyte.oauth.RESPONSE_TYPE_KEY
import io.airbyte.oauth.SCOPE_KEY
import org.apache.http.client.utils.URIBuilder
import java.io.IOException
import java.net.URISyntaxException
import java.net.http.HttpClient
import java.util.UUID

/**
 * Following docs from https://developer.zendesk.com/documentation/live-chat/getting-started/auth/.
 */
class ZendeskChatOAuthFlow(
  httpClient: HttpClient,
) : BaseOAuth2Flow(httpClient) {
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
        .setHost("www.zopim.com")
        .setPath("oauth2/authorizations/new") // required
        .addParameter(CLIENT_ID_KEY, clientId)
        .addParameter(REDIRECT_URI_KEY, redirectUrl)
        .addParameter(RESPONSE_TYPE_KEY, AUTH_CODE_KEY)
        .addParameter(SCOPE_KEY, "read chat")
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
      GRANT_TYPE_KEY to "authorization_code",
      AUTH_CODE_KEY to authCode,
      CLIENT_ID_KEY to clientId,
      CLIENT_SECRET_KEY to clientSecret,
      REDIRECT_URI_KEY to redirectUrl,
      SCOPE_KEY to "read",
    )

  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String = ACCESS_TOKEN_URL

  override fun extractOAuthOutput(
    data: JsonNode,
    accessTokenUrl: String,
  ): Map<String, Any> {
    val result: MutableMap<String, Any> = mutableMapOf()
    // getting out access_token
    if (data.has("access_token")) {
      result["access_token"] = data["access_token"].asText()
    } else {
      throw IOException(String.format("Missing 'access_token' in query params from %s", accessTokenUrl))
    }
    // getting out refresh_token
    if (data.has(REFRESH_TOKEN_KEY)) {
      result[REFRESH_TOKEN_KEY] = data[REFRESH_TOKEN_KEY].asText()
    } else {
      throw IOException("Missing 'refresh_token' in query params from $accessTokenUrl")
    }
    return result
  }

  companion object {
    private const val ACCESS_TOKEN_URL = "https://www.zopim.com/oauth2/token"
  }
}
