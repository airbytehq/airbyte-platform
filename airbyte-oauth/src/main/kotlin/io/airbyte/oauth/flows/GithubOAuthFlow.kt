/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
import io.airbyte.oauth.BaseOAuth2Flow
import org.apache.http.client.utils.URIBuilder
import java.io.IOException
import java.net.URISyntaxException
import java.net.http.HttpClient
import java.util.UUID
import java.util.function.Supplier

/**
 * Following docs from
 * https://docs.github.com/en/developers/apps/building-oauth-apps/authorizing-oauth-apps#web-application-flow.
 */
class GithubOAuthFlow : BaseOAuth2Flow {
  val scopes: String get() = java.lang.String.join("%20", SCOPES)

  constructor(httpClient: HttpClient) : super(httpClient)

  @VisibleForTesting
  internal constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier)

  @Throws(IOException::class)
  override fun formatConsentUrl(
    definitionId: UUID?,
    clientId: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
  ): String {
    try {
      return URIBuilder(AUTHORIZE_URL)
        .addParameter("client_id", clientId)
        .addParameter(
          "redirect_uri",
          redirectUrl,
        ) // we add `scopes` and `state` after we've already built the url, to prevent url encoding for scopes
        // https://docs.github.com/en/developers/apps/building-oauth-apps/scopes-for-oauth-apps#available-scopes
        // we need to keep scopes in the format of: < scope1%20scope2:sub_scope%20scope3 >
        .build()
        .toString() + "&scope=" + scopes + "&state=" + getState()
    } catch (e: URISyntaxException) {
      throw IOException("Failed to format Consent URL for OAuth flow", e)
    }
  }

  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String = ACCESS_TOKEN_URL

  @Throws(IOException::class)
  override fun extractOAuthOutput(
    data: JsonNode,
    accessTokenUrl: String,
  ): Map<String, Any> {
    if (data.has("access_token")) {
      return java.util.Map.of<String, Any>("access_token", data["access_token"].asText())
    } else {
      throw IOException(String.format("Missing 'access_token' in query params from %s", ACCESS_TOKEN_URL))
    }
  }

  companion object {
    private const val AUTHORIZE_URL = "https://github.com/login/oauth/authorize"
    private const val ACCESS_TOKEN_URL = "https://github.com/login/oauth/access_token"

    // Setting "repo" scope would allow grant not only read but also write
    // access to our application. Unfortunately we cannot follow least
    // privilege principle here cause GitHub has no option of granular access
    // tune up.
    // This is necessary to pull data from private repositories.
    // https://docs.github.com/en/developers/apps/building-oauth-apps/scopes-for-oauth-apps#available-scopes
    private val SCOPES: List<String> =
      mutableListOf(
        "repo",
        "read:org",
        "read:repo_hook",
        "read:user",
        "read:project",
        "read:discussion",
        "workflow",
      )
  }
}
