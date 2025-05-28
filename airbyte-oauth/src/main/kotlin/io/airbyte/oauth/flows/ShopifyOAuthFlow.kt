/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.collect.ImmutableMap
import io.airbyte.commons.json.Jsons
import io.airbyte.oauth.BaseOAuth2Flow
import io.airbyte.protocol.models.v0.OAuthConfigSpecification
import io.airbyte.validation.json.JsonValidationException
import org.apache.http.client.utils.URIBuilder
import java.io.IOException
import java.net.URI
import java.net.URISyntaxException
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.UUID

/**
 * Shopify Oauth.
 */
class ShopifyOAuthFlow(
  httpClient: HttpClient,
) : BaseOAuth2Flow(httpClient) {
  @Throws(IOException::class)
  override fun formatConsentUrl(
    definitionId: UUID?,
    clientId: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
  ): String {
        /*
         * Build the URL that leads to the Shopify Marketplace showing the `Airbyte` application to install.
         */
    val builder =
      URIBuilder()
        .setScheme("https")
        .setHost("apps.shopify.com")
        .setPath("airbyte")

    try {
      return builder.build().toString()
    } catch (e: URISyntaxException) {
      throw IOException("Failed to format Consent URL for OAuth flow", e)
    }
  }

  @Throws(IOException::class, JsonValidationException::class)
  override fun completeSourceOAuth(
    workspaceId: UUID,
    sourceDefinitionId: UUID?,
    queryParams: Map<String, Any>,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
    oauthConfigSpecification: OAuthConfigSpecification,
    oauthParamConfig: JsonNode,
  ): Map<String, Any> {
    validateInputOAuthConfiguration(oauthConfigSpecification, inputOAuthConfiguration)
    if (containsIgnoredOAuthError(queryParams)) {
      return buildRequestError(queryParams)
    }
    return formatOAuthOutput(
      oauthParamConfig,
      completeOAuthFlow(
        getClientIdUnsafe(oauthParamConfig),
        getClientSecretUnsafe(oauthParamConfig),
        extractCodeParameter(queryParams),
        extractShopParameter(queryParams),
        redirectUrl,
        inputOAuthConfiguration,
        oauthParamConfig,
      ),
      oauthConfigSpecification,
    )
  }

  @Throws(IOException::class)
  protected fun completeOAuthFlow(
    clientId: String,
    clientSecret: String,
    authCode: String,
    shopName: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode?,
    oauthParamConfig: JsonNode?,
  ): Map<String, Any> {
    val accessTokenUrl = formatAccessTokenUrl(shopName)
    val request =
      HttpRequest
        .newBuilder()
        .POST(
          HttpRequest.BodyPublishers
            .ofString(
              tokenReqContentType.converter.apply(
                getAccessTokenQueryParameters(clientId, clientSecret, authCode, redirectUrl),
              ),
            ),
        ).uri(URI.create(accessTokenUrl))
        .header("Content-Type", tokenReqContentType.contentType)
        .header("Accept", "application/json")
        .build()
    try {
      val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
      return extractOAuthOutput(Jsons.deserialize(response.body()), accessTokenUrl, shopName)
    } catch (e: InterruptedException) {
      throw IOException("Failed to complete OAuth flow", e)
    }
  }

  override fun getAccessTokenQueryParameters(
    clientId: String,
    clientSecret: String,
    authCode: String,
    redirectUrl: String,
  ): Map<String, String> =
    ImmutableMap
      .builder<String, String>()
      .put("client_id", clientId)
      .put("client_secret", clientSecret)
      .put("code", authCode)
      .build()

  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String = ""

  private fun formatAccessTokenUrl(shopName: String?): String {
    // building the access_token_url with the shop name
    return "https://$shopName/admin/oauth/access_token"
  }

  @Throws(IOException::class)
  private fun extractShopParameter(queryParams: Map<String, Any>): String {
    if (queryParams.containsKey(SHOP)) {
      return queryParams[SHOP] as String
    } else {
      throw IOException("Undefined 'shop' from consent redirected url.")
    }
  }

  @Throws(IOException::class)
  protected fun extractOAuthOutput(
    data: JsonNode,
    accessTokenUrl: String,
    shopName: String,
  ): Map<String, Any> {
    val result: MutableMap<String, Any> = HashMap()
    // getting out access_token
    if (data.has("access_token")) {
      result["access_token"] = data["access_token"].asText()
    } else {
      throw IOException(String.format("Missing 'access_token' in query params from %s", accessTokenUrl))
    }
    result["shop"] = shopName
    return result
  }

  companion object {
    private const val SHOP = "shop"
  }
}
