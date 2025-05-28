/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.collect.ImmutableMap
import io.airbyte.oauth.BaseOAuth2Flow
import org.apache.http.client.utils.URIBuilder
import java.io.IOException
import java.net.URISyntaxException
import java.net.http.HttpClient
import java.time.Clock
import java.time.Instant
import java.util.UUID
import java.util.function.Supplier

/**
 * Smartsheets OAuth.
 */
class SmartsheetsOAuthFlow : BaseOAuth2Flow {
  private val clock: Clock

  constructor(httpClient: HttpClient) : super(httpClient) {
    this.clock = Clock.systemUTC()
  }

  constructor(
    httpClient: HttpClient,
    stateSupplier: Supplier<String>,
    clock: Clock,
  ) : super(httpClient, stateSupplier) {
    this.clock = clock
  }

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
        .addParameter("response_type", "code")
        .addParameter("state", getState())
        .addParameter("scope", "READ_SHEETS")
        .build()
        .toString()
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
  ): Map<String, String> =
    ImmutableMap
      .builder<String, String>() // required
      .put("grant_type", "authorization_code")
      .put("client_id", clientId)
      .put("client_secret", clientSecret)
      .put("code", authCode)
      .build()

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

  @Deprecated("")
  @Throws(IOException::class)
  override fun completeSourceOAuth(
    workspaceId: UUID,
    sourceDefinitionId: UUID?,
    queryParams: Map<String, Any>,
    redirectUrl: String,
    oauthParamConfig: JsonNode,
  ): Map<String, Any> = throw IOException("Deprecated API not supported by this connector")

  companion object {
    private const val AUTHORIZE_URL = "https://app.smartsheet.com/b/authorize"
    private const val ACCESS_TOKEN_URL = "https://api.smartsheet.com/2.0/token"
  }
}
