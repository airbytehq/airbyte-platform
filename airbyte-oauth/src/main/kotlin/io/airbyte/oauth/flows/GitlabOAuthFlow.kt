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
import java.time.Instant
import java.util.UUID
import java.util.function.Supplier

/**
 * Following docs from https://docs.gitlab.com/ee/api/oauth2.html#authorization-code-flow.
 */
class GitlabOAuthFlow : BaseOAuth2Flow {
  constructor(httpClient: HttpClient) : super(httpClient)

  constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier)

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
        .setHost(getDomain(inputOAuthConfiguration))
        .setPath("oauth/authorize")
        .addParameter("client_id", clientId)
        .addParameter("redirect_uri", redirectUrl)
        .addParameter("state", getState())
        .addParameter("response_type", "code")
        .addParameter("scope", "read_api")
    try {
      return builder.build().toString()
    } catch (e: URISyntaxException) {
      throw IOException("Failed to format Consent URL for OAuth flow", e)
    }
  }

  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String {
    val domain = getDomain(inputOAuthConfiguration)
    return String.format(ACCESS_TOKEN_URL, domain)
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
      .put("grant_type", "authorization_code")
      .put("redirect_uri", redirectUrl)
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
      val expiresIn = Instant.ofEpochSecond(data["created_at"].asInt().toLong()).plusSeconds(data["expires_in"].asInt().toLong())
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
    private const val ACCESS_TOKEN_URL = "https://%s/oauth/token"
    private const val DEFAULT_GITLAB_DOMAIN = "gitlab.com"

    protected fun getDomain(inputOAuthConfiguration: JsonNode): String {
      var stringURL = DEFAULT_GITLAB_DOMAIN
      if (inputOAuthConfiguration.has("domain")) {
        val url = inputOAuthConfiguration["domain"].asText()
        if (url.isNotEmpty()) {
          stringURL = url
        }
      }
      // this could be `https://gitlab.com` or `gitlab.com`
      // because the connector supports storing hostname with and without schema
      val parts = stringURL.split("//".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
      return parts[parts.size - 1]
    }
  }
}
