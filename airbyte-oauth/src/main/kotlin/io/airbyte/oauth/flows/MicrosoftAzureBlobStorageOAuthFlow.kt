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
import org.apache.http.client.utils.URIBuilder
import java.io.IOException
import java.net.URISyntaxException
import java.net.http.HttpClient
import java.util.UUID
import java.util.function.Supplier

/**
 * Microsoft Azure Blob Storage OAuth.
 */
class MicrosoftAzureBlobStorageOAuthFlow : BaseOAuth2Flow {
  constructor(httpClient: HttpClient) : super(httpClient)

  @InternalForTesting
  constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier)

  private val scopes: String get() = "offline_access%20https://storage.azure.com/.default"

  override fun formatConsentUrl(
    definitionId: UUID?,
    clientId: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
  ): String {
    val tenantId: String
    try {
      tenantId = getConfigValueUnsafe(inputOAuthConfiguration, FIELD_NAME)
    } catch (e: IllegalArgumentException) {
      throw IOException("Failed to get $FIELD_NAME value from input configuration", e)
    }

    try {
      return URIBuilder()
        .setScheme("https")
        .setHost("login.microsoftonline.com")
        .setPath("$tenantId/oauth2/v2.0/authorize")
        .addParameter(CLIENT_ID_KEY, clientId)
        .addParameter(RESPONSE_TYPE_KEY, AUTH_CODE_KEY)
        .addParameter(REDIRECT_URI_KEY, redirectUrl)
        .addParameter("response_mode", "query")
        .addParameter("state", getState())
        .build()
        .toString() + "&scope=" + scopes
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
      CLIENT_SECRET_KEY to clientSecret,
      AUTH_CODE_KEY to authCode,
      REDIRECT_URI_KEY to redirectUrl,
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
