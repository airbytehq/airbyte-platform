/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
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

/**
 * Microsoft Teams OAuth flow.
 */
class MicrosoftTeamsOAuthFlow : BaseOAuth2Flow {
  constructor(httpClient: HttpClient) : super(httpClient)

  constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier, TokenRequestContentType.JSON)

  /**
   * Depending on the OAuth flow implementation, the URL to grant user's consent may differ,
   * especially in the query parameters to be provided. This function should generate such consent URL
   * accordingly.
   *
   * @param definitionId The configured definition ID of this client
   * @param clientId The configured client ID
   * @param redirectUrl the redirect URL
   */
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
      throw IOException("Failed to format Consent URL for OAuth flow", e)
    }

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
      REDIRECT_URI_KEY to redirectUrl,
      CLIENT_SECRET_KEY to clientSecret,
      AUTH_CODE_KEY to authCode,
      GRANT_TYPE_KEY to "authorization_code",
    )

  private val scopes: String
    get() =
      listOf(
        "offline_access",
        "Application.Read.All",
        "Channel.ReadBasic.All",
        "ChannelMember.Read.All",
        "ChannelMember.ReadWrite.All",
        "ChannelSettings.Read.All",
        "ChannelSettings.ReadWrite.All",
        "Directory.Read.All",
        "Directory.ReadWrite.All",
        "Files.Read.All",
        "Files.ReadWrite.All",
        "Group.Read.All",
        "Group.ReadWrite.All",
        "GroupMember.Read.All",
        "Reports.Read.All",
        "Sites.Read.All",
        "Sites.ReadWrite.All",
        "TeamsTab.Read.All",
        "TeamsTab.ReadWrite.All",
        "User.Read.All",
        "User.ReadWrite.All",
      ).joinToString(" ")

  @Deprecated("")
  override fun completeSourceOAuth(
    workspaceId: UUID,
    sourceDefinitionId: UUID?,
    queryParams: Map<String, Any>,
    redirectUrl: String,
    oauthParamConfig: JsonNode,
  ): Map<String, Any> =
    throw IOException("Using the deprecated OAuth methods is not supported. This OAuth flow depends on values defined in connector configs")

  @Deprecated("")
  override fun completeDestinationOAuth(
    workspaceId: UUID,
    destinationDefinitionId: UUID?,
    queryParams: Map<String, Any>,
    redirectUrl: String,
    oauthParamConfig: JsonNode,
  ): Map<String, Any> =
    throw IOException("Using the deprecated OAuth methods is not supported. This OAuth flow depends on values defined in connector configs")

  /**
   * Returns the URL where to retrieve the access token from.
   *
   */
  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String {
    val tenantId = getConfigValueUnsafe(inputOAuthConfiguration, FIELD_NAME)
    return "https://login.microsoftonline.com/$tenantId/oauth2/v2.0/token"
  }

  companion object {
    private const val FIELD_NAME = "tenant_id"
  }
}
