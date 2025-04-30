/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
import com.google.common.collect.ImmutableMap
import io.airbyte.oauth.BaseOAuth2Flow
import org.apache.http.client.utils.URIBuilder
import java.io.IOException
import java.net.URISyntaxException
import java.net.http.HttpClient
import java.util.UUID
import java.util.function.Supplier

/**
 * Following docs from
 * https://help.salesforce.com/s/articleView?language=en_US&amp;id=sf.remoteaccess_oauth_web_server_flow.htm.
 */
class SalesforceOAuthFlow : BaseOAuth2Flow {
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
      return URIBuilder(
        String.format(io.airbyte.oauth.flows.SalesforceOAuthFlow.Companion.AUTHORIZE_URL, getEnvironment(inputOAuthConfiguration)),
      ).addParameter("client_id", clientId)
        .addParameter("redirect_uri", redirectUrl)
        .addParameter("response_type", "code")
        .addParameter("state", getState())
        .build()
        .toString()
    } catch (e: URISyntaxException) {
      throw IOException("Failed to format Consent URL for OAuth flow", e)
    }
  }

  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String =
    String.format(io.airbyte.oauth.flows.SalesforceOAuthFlow.Companion.ACCESS_TOKEN_URL, getEnvironment(inputOAuthConfiguration))

  override fun getAccessTokenQueryParameters(
    clientId: String,
    clientSecret: String,
    authCode: String,
    redirectUrl: String,
  ): Map<String, String> =
    ImmutableMap
      .builder<String, String>()
      .putAll(super.getAccessTokenQueryParameters(clientId, clientSecret, authCode, redirectUrl))
      .put("grant_type", "authorization_code")
      .build()

  override fun getDefaultOAuthOutputPath(): List<String> = listOf()

  private fun getEnvironment(inputOAuthConfiguration: JsonNode): String {
    val isSandbox = inputOAuthConfiguration["is_sandbox"] ?: return "login"
    return if (isSandbox.asBoolean() == true) "test" else "login"
  }

  companion object {
    // Clickable link for IDE
    // https://help.salesforce.com/s/articleView?language=en_US&id=sf.remoteaccess_oauth_web_server_flow.htm
    private const val AUTHORIZE_URL = "https://%s.salesforce.com/services/oauth2/authorize"
    private const val ACCESS_TOKEN_URL = "https://%s.salesforce.com/services/oauth2/token"
  }
}
