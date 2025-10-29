/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows.facebook

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.oauth.BaseOAuth2Flow
import io.airbyte.oauth.CLIENT_ID_KEY
import io.airbyte.oauth.CLIENT_SECRET_KEY
import io.airbyte.oauth.GRANT_TYPE_KEY
import io.airbyte.oauth.REDIRECT_URI_KEY
import io.airbyte.oauth.SCOPE_KEY
import org.apache.http.client.utils.URIBuilder
import java.io.IOException
import java.net.URI
import java.net.URISyntaxException
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.UUID
import java.util.function.Supplier

/**
 * Following docs from
 * https://developers.facebook.com/docs/facebook-login/manually-build-a-login-flow.
 */
abstract class FacebookOAuthFlow : BaseOAuth2Flow {
  constructor(httpClient: HttpClient) : super(httpClient)

  internal constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier)

  protected abstract fun getScopes(): String

  override fun formatConsentUrl(
    definitionId: UUID?,
    clientId: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
  ): String {
    try {
      return URIBuilder(AUTH_CODE_TOKEN_URL)
        .addParameter(CLIENT_ID_KEY, clientId)
        .addParameter(REDIRECT_URI_KEY, redirectUrl)
        .addParameter("state", getState())
        .addParameter(SCOPE_KEY, getScopes())
        .build()
        .toString()
    } catch (e: URISyntaxException) {
      throw IOException("Failed to format Consent URL for OAuth flow", e)
    }
  }

  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String = ACCESS_TOKEN_URL

  override fun extractOAuthOutput(
    data: JsonNode,
    accessTokenUrl: String,
  ): Map<String, Any> {
    // Facebook does not have refresh token but calls it "long lived access token" instead:
    // see https://developers.facebook.com/docs/facebook-login/access-tokens/refreshing
    require(data.has(ACCESS_TOKEN)) { "Missing 'access_token' in query params from $ACCESS_TOKEN_URL" }
    return java.util.Map.of<String, Any>(ACCESS_TOKEN, data[ACCESS_TOKEN].asText())
  }

  override fun completeOAuthFlow(
    clientId: String,
    clientSecret: String,
    authCode: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
    oauthParamConfig: JsonNode,
    state: String?,
  ): Map<String, Any> {
    // Access tokens generated via web login are short-lived tokens
    // they arre valid for 1 hour and need to be exchanged for long-lived access token
    // https://developers.facebook.com/docs/facebook-login/access-tokens (Short-Term Tokens and
    // https://developers.facebook.com/docs/instagram-basic-display-api/overview#short-lived-access-tokens
    // Long-Term Tokens section)

    val data =
      super.completeOAuthFlow(clientId, clientSecret, authCode, redirectUrl, inputOAuthConfiguration, oauthParamConfig, state)
    require(data.containsKey(ACCESS_TOKEN))
    val shortLivedAccessToken = data[ACCESS_TOKEN] as String?
    val longLivedAccessToken = getLongLivedAccessToken(clientId, clientSecret, shortLivedAccessToken)
    return java.util.Map.of<String, Any>(ACCESS_TOKEN, longLivedAccessToken)
  }

  protected fun createLongLivedTokenURI(
    clientId: String?,
    clientSecret: String?,
    shortLivedAccessToken: String?,
  ): URI {
    // Exchange Short-lived Access token for Long-lived one
    // https://developers.facebook.com/docs/facebook-login/access-tokens/refreshing
    // It's valid for 60 days and resreshed once per day if using in requests.
    // If no requests are made, the token will expire after about 60 days and
    // the person will have to go through the login flow again to get a new
    // token.
    return URIBuilder(ACCESS_TOKEN_URL)
      .addParameter(CLIENT_SECRET_KEY, clientSecret)
      .addParameter(CLIENT_ID_KEY, clientId)
      .addParameter(GRANT_TYPE_KEY, "fb_exchange_token")
      .addParameter("fb_exchange_token", shortLivedAccessToken)
      .build()
  }

  protected fun getLongLivedAccessToken(
    clientId: String?,
    clientSecret: String?,
    shortLivedAccessToken: String?,
  ): String {
    try {
      val uri = createLongLivedTokenURI(clientId, clientSecret, shortLivedAccessToken)
      val request =
        HttpRequest
          .newBuilder()
          .GET()
          .uri(uri)
          .build()
      val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
      val responseJson = Jsons.deserialize(response.body())
      require(responseJson.hasNonNull(ACCESS_TOKEN)) { "$responseJson response should have access_token" }
      return responseJson[ACCESS_TOKEN].asText()
    } catch (e: InterruptedException) {
      throw IOException("Failed to complete OAuth flow", e)
    } catch (e: URISyntaxException) {
      throw IOException("Failed to complete OAuth flow", e)
    }
  }

  @Deprecated("")
  override fun getDefaultOAuthOutputPath(): List<String> = listOf()

  companion object {
    private const val ACCESS_TOKEN_URL = "https://graph.facebook.com/v23.0/oauth/access_token"
    private const val AUTH_CODE_TOKEN_URL = "https://www.facebook.com/v23.0/dialog/oauth"
    private const val ACCESS_TOKEN = "access_token"
  }
}
