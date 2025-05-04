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
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.security.SecureRandom
import java.time.Clock
import java.time.Instant
import java.util.Base64
import java.util.UUID

/**
 * Following docs from
 * https://airtable.com/developers/web/api/oauth-reference#authorization-request-query.
 */
class AirtableOAuthFlow(
  httpClient: HttpClient,
) : BaseOAuth2Flow(httpClient) {
  private val clock: Clock = Clock.systemUTC()
  private val secureRandom = SecureRandom()
  val scopes: String
    get() = // More info and additional scopes could be found here:
      // https://airtable.com/developers/web/api/scopes
      // should be space-delimitered
      java.lang.String.join(" ", SCOPES)

  val codeVerifier: String
    /**
     * Must be a cryptographically generated string; 43-128 characters long.
     * https://airtable.com/developers/web/api/oauth-reference#authorization-parameter-rules
     */
    get() {
      val allowedCharacters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_"
      val length = secureRandom.nextInt((128 - 43) + 1) + 43
      val sb = StringBuilder(length)
      for (i in 0..<length) {
        val randomIndex = secureRandom.nextInt(allowedCharacters.length)
        val randomChar = allowedCharacters[randomIndex]
        sb.append(randomChar)
      }
      return sb.toString()
    }

  /**
   * Base64 url encoding of the sha256 hash of code_verifier.
   * https://airtable.com/developers/web/api/oauth-reference#authorization-parameter-rules
   */
  @Throws(IOException::class)
  fun getCodeChallenge(codeVerifier: String): String {
    try {
      val messageDigest = MessageDigest.getInstance("SHA-256")
      messageDigest.update(codeVerifier.toByteArray(StandardCharsets.UTF_8))
      val codeChallengeBytes = messageDigest.digest()
      return Base64.getUrlEncoder().withoutPadding().encodeToString(codeChallengeBytes)
    } catch (e: NoSuchAlgorithmException) {
      throw IOException("Failed to get code_challenge for OAuth flow", e)
    }
  }

  @Throws(IOException::class)
  override fun formatConsentUrl(
    definitionId: UUID?,
    clientId: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
  ): String {
    val codeVerifier = codeVerifier

    val builder =
      URIBuilder()
        .setScheme("https")
        .setHost("airtable.com")
        .setPath("oauth2/v1/authorize") // required
        .addParameter("redirect_uri", redirectUrl)
        .addParameter("client_id", clientId)
        .addParameter("response_type", "code")
        .addParameter("scope", scopes)
        .addParameter("code_challenge", getCodeChallenge(codeVerifier))
        .addParameter("code_challenge_method", "S256")
        .addParameter("state", codeVerifier)

    try {
      return builder.build().toString()
    } catch (e: URISyntaxException) {
      throw IOException("Failed to format Consent URL for OAuth flow", e)
    }
  }

  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String = ACCESS_TOKEN_URL

  protected fun getAccessTokenQueryParameters(
    clientId: String,
    clientSecret: String?,
    authCode: String,
    state: String,
    redirectUrl: String,
  ): Map<String, String> =
    ImmutableMap
      .builder<String, String>() // required
      .put("code", authCode)
      .put("redirect_uri", redirectUrl)
      .put("grant_type", "authorization_code")
      .put("client_id", clientId)
      .put("code_verifier", state)
      .put("code_challenge_method", "S256")
      .build()

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
        extractStateParameter(queryParams)!!,
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
    state: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
    oauthParamConfig: JsonNode?,
  ): Map<String, Any> {
    val accessTokenUrl = getAccessTokenUrl(inputOAuthConfiguration)
    val authorization =
      Base64
        .getEncoder()
        .encode(("$clientId:$clientSecret").toByteArray(StandardCharsets.UTF_8))
    val request =
      HttpRequest
        .newBuilder()
        .POST(
          HttpRequest.BodyPublishers
            .ofString(
              tokenReqContentType.converter.apply(
                getAccessTokenQueryParameters(clientId, clientSecret, authCode, state, redirectUrl),
              ),
            ),
        ).uri(URI.create(accessTokenUrl))
        .header("Content-Type", tokenReqContentType.contentType)
        .header("Authorization", "Basic " + String(authorization, StandardCharsets.UTF_8))
        .build()
    try {
      val response =
        httpClient.send(
          request,
          HttpResponse.BodyHandlers.ofString(),
        )

      return extractOAuthOutput(Jsons.deserialize(response.body()), accessTokenUrl)
    } catch (e: InterruptedException) {
      throw IOException("Failed to complete OAuth flow", e)
    }
  }

  @Throws(IOException::class)
  override fun extractOAuthOutput(
    data: JsonNode,
    accessTokenUrl: String,
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
    return result
  }

  /**
   * This function should parse and extract the state from these query parameters in order to continue
   * the OAuth Flow.
   */
  @Throws(IOException::class)
  protected fun extractStateParameter(queryParams: Map<String, Any>): String? {
    if (queryParams.containsKey("state")) {
      return queryParams["state"] as String?
    } else {
      throw IOException("Undefined 'state' from consent redirected url.")
    }
  }

  companion object {
    private const val ACCESS_TOKEN_URL = "https://airtable.com/oauth2/v1/token"
    private val SCOPES: List<String> =
      mutableListOf(
        "data.records:read",
        "data.recordComments:read",
        "schema.bases:read",
      )
  }
}
