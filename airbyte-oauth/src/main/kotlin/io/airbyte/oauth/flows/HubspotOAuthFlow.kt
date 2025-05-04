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
import java.util.UUID
import java.util.function.Supplier

/**
 * Hubspot OAuth.
 */
class HubspotOAuthFlow : BaseOAuth2Flow {
  constructor(httpClient: HttpClient) : super(httpClient)

  constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier, TokenRequestContentType.JSON)

  @Throws(IOException::class)
  override fun formatConsentUrl(
    definitionId: UUID?,
    clientId: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
  ): String {
    try {
            /*
             * Not all accounts have access to all scopes so we're requesting them as optional. Hubspot still
             * expects scopes to be defined, so the contacts scope is left as required as it is accessible by
             * any marketing or CRM account according to
             * https://legacydocs.hubspot.com/docs/methods/oauth2/initiate-oauth-integration#scopes
             */
      return URIBuilder(AUTHORIZE_URL)
        .addParameter("client_id", clientId)
        .addParameter("redirect_uri", redirectUrl)
        .addParameter("state", getState())
        .addParameter("scopes", requiredScopes)
        .addParameter("optional_scopes", optionalScopes)
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
    ImmutableMap
      .builder<String, String>() // required
      .put("client_id", clientId)
      .put("redirect_uri", redirectUrl)
      .put("client_secret", clientSecret)
      .put("code", authCode)
      .put("grant_type", "authorization_code")
      .build()

  private val optionalScopes: String
    get() =
      java.lang.String.join(
        " ",
        "content",
        "automation",
        "e-commerce",
        "files",
        "files.ui_hidden.read",
        "forms",
        "forms-uploaded-files",
        "sales-email-read",
        "tickets",
        "crm.lists.read",
        "crm.objects.companies.read",
        "crm.objects.custom.read",
        "crm.objects.deals.read",
        "crm.objects.feedback_submissions.read",
        "crm.objects.goals.read",
        "crm.objects.owners.read",
        "crm.schemas.companies.read",
        "crm.schemas.custom.read",
        "crm.schemas.deals.read",
        "crm.objects.leads.read",
      )

  private val requiredScopes: String
    get() = "crm.schemas.contacts.read+crm.objects.contacts.read"

  /**
   * Returns the URL where to retrieve the access token from.
   */
  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String = "https://api.hubapi.com/oauth/v1/token"

  companion object {
    private const val AUTHORIZE_URL = "https://app.hubspot.com/oauth/authorize"
  }
}
