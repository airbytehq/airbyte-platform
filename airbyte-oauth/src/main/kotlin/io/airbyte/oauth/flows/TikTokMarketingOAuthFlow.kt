/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
import com.google.common.collect.ImmutableMap
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import io.airbyte.commons.json.Jsons
import io.airbyte.oauth.BaseOAuth2Flow
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
 * https://ads.tiktok.com/marketing_api/docs?id=1701890912382977&is_new_connect=0&is_new_user=0&rid=sta6fe2yww.
 *
 * What tiktok calls appId we call clientId.
 */
class TikTokMarketingOAuthFlow : BaseOAuth2Flow {
  override fun getClientIdUnsafe(oauthConfig: JsonNode): String = getConfigValueUnsafe(oauthConfig, "app_id")

  override fun getClientSecretUnsafe(oauthConfig: JsonNode): String = getConfigValueUnsafe(oauthConfig, "secret")

  constructor(httpClient: HttpClient) : super(httpClient)

  @VisibleForTesting
  constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier, TokenRequestContentType.JSON)

  @Throws(IOException::class)
  override fun completeOAuthFlow(
    clientId: String,
    clientSecret: String,
    authCode: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
    oauthParamConfig: JsonNode,
    state: String?,
  ): Map<String, Any> {
    val accessTokenUrl = getAccessTokenUrl(inputOAuthConfiguration)
    val request =
      HttpRequest
        .newBuilder()
        .POST(
          HttpRequest.BodyPublishers
            .ofString(toJson(getAccessTokenQueryParameters(clientId, clientSecret, authCode, redirectUrl))),
        ).uri(URI.create(accessTokenUrl))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .build()
    // TODO: Handle error response to report better messages
    try {
      val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
      return extractOAuthOutput(Jsons.deserialize(response.body()), accessTokenUrl)
    } catch (e: InterruptedException) {
      throw IOException("Failed to complete OAuth flow", e)
    }
  }

  @Throws(IOException::class)
  override fun formatConsentUrl(
    definitionId: UUID?,
    clientId: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
  ): String {
    val builder =
      URIBuilder()
        .setScheme("https")
        .setHost("ads.tiktok.com")
        .setPath("marketing_api/auth") // required
        .addParameter("app_id", clientId)
        .addParameter("redirect_uri", redirectUrl)
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
    ImmutableMap
      .builder<String, String>() // required
      .put("auth_code", authCode)
      .put("app_id", clientId)
      .put("secret", clientSecret)
      .build()

  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String = ACCESS_TOKEN_URL

  @Throws(IOException::class)
  override fun extractCodeParameter(queryParams: Map<String, Any>): String =
    if (queryParams.containsKey("auth_code")) {
      queryParams["auth_code"] as String
    } else if (queryParams.containsKey("code")) {
      queryParams["code"] as String
    } else {
      throw IOException("Undefined 'auth_code'/'code' from consent redirected url.")
    }

  @Throws(IOException::class)
  override fun extractOAuthOutput(
    data: JsonNode,
    accessTokenUrl: String,
  ): Map<String, Any> {
    val result: MutableMap<String, Any> = HashMap()
    // getting out access_token
    if ((data.has("data")) && (data["data"].has("access_token"))) {
      result["access_token"] = data["data"]["access_token"].asText()
    } else {
      throw IOException(String.format("Missing 'access_token' in query params from %s, data: %s", accessTokenUrl, data.toString()))
    }
    return result
  }

  companion object {
    private const val ACCESS_TOKEN_URL = "https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token/"

    protected fun toJson(body: Map<String, String>?): String {
      val gson = Gson()
      val gsonType = object : TypeToken<Map<String?, String?>?>() {}.type
      return gson.toJson(body, gsonType)
    }
  }
}
