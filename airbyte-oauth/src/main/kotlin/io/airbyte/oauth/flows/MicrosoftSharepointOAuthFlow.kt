/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.oauth.AUTH_CODE_KEY
import io.airbyte.oauth.BaseOAuth2Flow
import io.airbyte.oauth.CLIENT_ID_KEY
import io.airbyte.oauth.CLIENT_SECRET_KEY
import io.airbyte.oauth.GRANT_TYPE_KEY
import io.airbyte.oauth.REDIRECT_URI_KEY
import io.airbyte.oauth.RESPONSE_TYPE_KEY
import io.airbyte.oauth.SCOPE_KEY
import org.apache.http.client.utils.URIBuilder
import java.io.IOException
import java.net.URISyntaxException
import java.net.http.HttpClient
import java.util.UUID
import java.util.function.Supplier

/** SharePoint OAuth.  */
class MicrosoftSharepointOAuthFlow : BaseOAuth2Flow {
  constructor(httpClient: HttpClient) : super(httpClient)

  @InternalForTesting
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
        .addParameter(CLIENT_ID_KEY, clientId)
        .addParameter(REDIRECT_URI_KEY, redirectUrl)
        .addParameter("state", getState())
        .addParameter(SCOPE_KEY, scopes)
        .addParameter(RESPONSE_TYPE_KEY, AUTH_CODE_KEY)
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
      CLIENT_ID_KEY to clientId,
      AUTH_CODE_KEY to authCode,
      REDIRECT_URI_KEY to redirectUrl,
      CLIENT_SECRET_KEY to clientSecret,
      GRANT_TYPE_KEY to "authorization_code",
    )

  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String {
    val tenantId = getConfigValueUnsafe(inputOAuthConfiguration, FIELD_NAME)
    return "https://login.microsoftonline.com/$tenantId/oauth2/v2.0/token"
  }

  companion object {
    private const val FIELD_NAME = "tenant_id"
  }
}
