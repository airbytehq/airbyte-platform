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

/** OneDrive OAuth.  */
class MicrosoftOneDriveOAuthFlow : BaseOAuth2Flow {
  constructor(httpClient: HttpClient) : super(httpClient)

  @VisibleForTesting
  constructor(
    httpClient: HttpClient,
    stateSupplier: Supplier<String>,
  ) : super(httpClient, stateSupplier)

  private val scopes: String
    get() = java.lang.String.join(" ", "offline_access", "Files.Read.All")

  override fun formatConsentUrl(
    definitionId: UUID?,
    clientId: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
  ): String {
    val tenantId = getConfigValueUnsafe(inputOAuthConfiguration, FIELD_NAME)

    try {
      return URIBuilder()
        .setScheme("https")
        .setHost("login.microsoftonline.com")
        .setPath("$tenantId/oauth2/v2.0/authorize")
        .addParameter("client_id", clientId)
        .addParameter("redirect_uri", redirectUrl)
        .addParameter("state", getState())
        .addParameter("scope", scopes)
        .addParameter("response_type", "code")
        .build()
        .toString()
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
    mapOf(
      "grant_type" to "authorization_code",
      "code" to authCode,
      "client_id" to clientId,
      "client_secret" to clientSecret,
      "redirect_uri" to redirectUrl,
    )

  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String {
    val tenantId = getConfigValueUnsafe(inputOAuthConfiguration, FIELD_NAME)
    return "https://login.microsoftonline.com/$tenantId/oauth2/v2.0/token"
  }

  companion object {
    private const val FIELD_NAME = "tenant_id"
  }
}
