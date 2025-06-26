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
import java.nio.charset.StandardCharsets
import java.time.Clock
import java.time.Instant
import java.util.Base64
import java.util.UUID
import java.util.function.Supplier

/**
 * Quickbooks OAuth.
 */
class QuickbooksOAuthFlow : BaseOAuth2Flow {
  private val clock: Clock?

  constructor(httpClient: HttpClient) : super(httpClient) {
    this.clock = Clock.systemUTC()
  }

  constructor(
    httpClient: HttpClient,
    stateSupplier: Supplier<String>,
    clock: Clock?,
  ) : this(httpClient, stateSupplier, TokenRequestContentType.JSON, clock)

  @JvmOverloads
  constructor(
    httpClient: HttpClient,
    stateSupplier: Supplier<String>,
    tokenReqContentType: TokenRequestContentType = TokenRequestContentType.JSON,
    clock: Clock? = Clock.systemUTC(),
  ) : super(httpClient, stateSupplier, tokenReqContentType) {
    this.clock = clock
  }

  val scopes: String
    get() = "com.intuit.quickbooks.accounting"

  @Throws(IOException::class)
  override fun formatConsentUrl(
    definitionId: UUID?,
    clientId: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
  ): String {
    try {
      return (
        URIBuilder(CONSENT_URL)
          .addParameter("client_id", clientId)
          .addParameter("scope", scopes)
          .addParameter("redirect_uri", redirectUrl)
          .addParameter("response_type", "code")
          .addParameter("state", getState())
          .build()
      ).toString()
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
      .put("redirect_uri", redirectUrl)
      .put("grant_type", "authorization_code")
      .put("code", authCode)
      .put("client_id", clientId)
      .put("client_secret", clientSecret)
      .build()

  /**
   * Returns the URL where to retrieve the access token from.
   */
  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String = TOKEN_URL

  @Throws(IOException::class)
  protected fun extractRealmIdParameter(queryParams: Map<String, Any>): String? {
    if (queryParams.containsKey("realmId")) {
      return queryParams["realmId"] as String?
    } else {
      throw IOException("Undefined 'realmId' from consent redirected url.")
    }
  }

  @Deprecated("")
  @Throws(IOException::class)
  override fun completeSourceOAuth(
    workspaceId: UUID,
    sourceDefinitionId: UUID?,
    queryParams: Map<String, Any>,
    redirectUrl: String,
    oauthParamConfig: JsonNode,
  ): Map<String, Any> {
    if (containsIgnoredOAuthError(queryParams)) {
      return buildRequestError(queryParams)
    }
    return formatOAuthOutput(
      oauthParamConfig,
      completeOAuthFlow(
        getClientIdUnsafe(oauthParamConfig),
        getClientSecretUnsafe(oauthParamConfig),
        extractCodeParameter(queryParams),
        extractRealmIdParameter(queryParams),
        redirectUrl,
        Jsons.emptyObject(),
        oauthParamConfig,
      ),
      getDefaultOAuthOutputPath(),
    )
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
        extractRealmIdParameter(queryParams),
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
    realmId: String?,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
    oauthParamConfig: JsonNode?,
  ): Map<String, Any> {
    val accessTokenUrl = getAccessTokenUrl(inputOAuthConfiguration)
    val request =
      HttpRequest
        .newBuilder()
        .POST(
          HttpRequest.BodyPublishers
            .ofString(tokenReqContentType.converter.apply(getAccessTokenQueryParameters(clientId, clientSecret, authCode, redirectUrl))),
        ).uri(URI.create(accessTokenUrl))
        .header("Content-Type", tokenReqContentType.contentType)
        .header("Accept", "application/json")
        .build()
    // TODO: Handle error response to report better messages
    try {
      val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
      return extractOAuthOutput(Jsons.deserialize(response.body()), accessTokenUrl, realmId)
    } catch (e: InterruptedException) {
      throw IOException("Failed to complete OAuth flow", e)
    }
  }

  @Throws(IOException::class)
  protected fun extractOAuthOutput(
    data: JsonNode,
    accessTokenUrl: String?,
    realmId: String?,
  ): Map<String, Any> {
    val result: MutableMap<String, Any> = HashMap()
    if (data.has("refresh_token")) {
      result["refresh_token"] = data["refresh_token"].asText()
    } else {
      throw IOException(String.format("Missing 'refresh_token' in query params from %s", accessTokenUrl))
    }
    if (data.has("access_token")) {
      result["access_token"] = data["access_token"].asText()
    } else {
      throw IOException(String.format("Missing 'access_token' in query params from %s", accessTokenUrl))
    }
    if (data.has("expires_in")) {
      val expiresIn = Instant.now(this.clock).plusSeconds(data["expires_in"].asInt().toLong())
      result["token_expiry_date"] = expiresIn.toString()
    } else {
      throw IOException(String.format("Missing 'expires_in' in query params from %s", accessTokenUrl))
    }
    result["realm_id"] = realmId as String
    return result
  }

  @Throws(IOException::class)
  override fun revokeSourceOauth(
    workspaceId: UUID,
    sourceDefinitionId: UUID,
    hydratedSourceConnectionConfiguration: JsonNode,
    oauthParamConfig: JsonNode,
  ) {
    val clientId = getClientIdUnsafe(oauthParamConfig)
    val clientSecret = getClientSecretUnsafe(oauthParamConfig)
    val authorization =
      Base64
        .getEncoder()
        .encodeToString(("$clientId:$clientSecret").toByteArray(StandardCharsets.UTF_8))
    val requestBody: MutableMap<String, String> = HashMap()
    val credentials = hydratedSourceConnectionConfiguration["credentials"]
    requestBody["token"] = getConfigValueUnsafe(credentials, "refresh_token")

    val request =
      HttpRequest
        .newBuilder()
        .POST(
          HttpRequest.BodyPublishers
            .ofString(tokenReqContentType.converter.apply(requestBody)),
        ).uri(URI.create(REVOKE_URL))
        .header("Content-Type", tokenReqContentType.contentType)
        .header("Accept", "application/json")
        .header("Authorization", "Basic $authorization")
        .build()
    try {
      httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    } catch (e: InterruptedException) {
      throw IOException("Failed to complete revocation request", e)
    }
  }

  companion object {
    private const val CONSENT_URL = "https://appcenter.intuit.com/app/connect/oauth2"
    private const val TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
    private const val REVOKE_URL = "https://developer.api.intuit.com/v2/oauth2/tokens/revoke"
  }
}
