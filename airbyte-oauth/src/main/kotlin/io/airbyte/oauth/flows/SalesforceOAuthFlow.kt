/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.oauth.AUTH_CODE_KEY
import io.airbyte.oauth.BaseOAuth2Flow
import io.airbyte.oauth.CLIENT_ID_KEY
import io.airbyte.oauth.GRANT_TYPE_KEY
import io.airbyte.oauth.REDIRECT_URI_KEY
import io.airbyte.oauth.RESPONSE_TYPE_KEY
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

  @InternalForTesting
  internal constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier)

  override fun formatConsentUrl(
    definitionId: UUID?,
    clientId: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
  ): String {
    try {
      return URIBuilder(
        String.format(io.airbyte.oauth.flows.SalesforceOAuthFlow.Companion.AUTHORIZE_URL, getEnvironment(inputOAuthConfiguration)),
      ).addParameter(CLIENT_ID_KEY, clientId)
        .addParameter(REDIRECT_URI_KEY, redirectUrl)
        .addParameter(RESPONSE_TYPE_KEY, AUTH_CODE_KEY)
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
    super.getAccessTokenQueryParameters(clientId, clientSecret, authCode, redirectUrl) +
      mapOf(GRANT_TYPE_KEY to "authorization_code")

  override fun getDefaultOAuthOutputPath(): List<String> = listOf()

  private fun getEnvironment(inputOAuthConfiguration: JsonNode): String {
    val isSandbox = inputOAuthConfiguration["is_sandbox"] ?: return "login"
    return if (isSandbox.asBoolean()) "test" else "login"
  }

  companion object {
    // Clickable link for IDE
    // https://help.salesforce.com/s/articleView?language=en_US&id=sf.remoteaccess_oauth_web_server_flow.htm
    private const val AUTHORIZE_URL = "https://%s.salesforce.com/services/oauth2/authorize"
    private const val ACCESS_TOKEN_URL = "https://%s.salesforce.com/services/oauth2/token"
  }
}
