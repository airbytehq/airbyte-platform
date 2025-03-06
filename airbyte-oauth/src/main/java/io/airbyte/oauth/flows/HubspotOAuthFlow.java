/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.oauth.BaseOAuth2Flow;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.http.client.utils.URIBuilder;

/**
 * Hubspot OAuth.
 */
public class HubspotOAuthFlow extends BaseOAuth2Flow {

  private static final String AUTHORIZE_URL = "https://app.hubspot.com/oauth/authorize";

  public HubspotOAuthFlow(final HttpClient httpClient) {
    super(httpClient);
  }

  public HubspotOAuthFlow(final HttpClient httpClient, final Supplier<String> stateSupplier) {
    super(httpClient, stateSupplier, TokenRequestContentType.JSON);
  }

  @Override
  protected String formatConsentUrl(final UUID definitionId,
                                    final String clientId,
                                    final String redirectUrl,
                                    final JsonNode inputOAuthConfiguration)
      throws IOException {
    try {
      /*
       * Not all accounts have access to all scopes so we're requesting them as optional. Hubspot still
       * expects scopes to be defined, so the contacts scope is left as required as it is accessible by
       * any marketing or CRM account according to
       * https://legacydocs.hubspot.com/docs/methods/oauth2/initiate-oauth-integration#scopes
       */
      return new URIBuilder(AUTHORIZE_URL)
          .addParameter("client_id", clientId)
          .addParameter("redirect_uri", redirectUrl)
          .addParameter("state", getState())
          .addParameter("scopes", getRequiredScopes())
          .addParameter("optional_scopes", getOptionalScopes())
          .build().toString();
    } catch (final URISyntaxException e) {
      throw new IOException("Failed to format Consent URL for OAuth flow", e);
    }
  }

  @Override
  protected Map<String, String> getAccessTokenQueryParameters(final String clientId,
                                                              final String clientSecret,
                                                              final String authCode,
                                                              final String redirectUrl) {
    return ImmutableMap.<String, String>builder()
        // required
        .put("client_id", clientId)
        .put("redirect_uri", redirectUrl)
        .put("client_secret", clientSecret)
        .put("code", authCode)
        .put("grant_type", "authorization_code")
        .build();
  }

  private String getOptionalScopes() {
    return String.join(" ", "content",
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
        "crm.objects.leads.read");
  }

  private String getRequiredScopes() {
    return "crm.schemas.contacts.read+crm.objects.contacts.read";
  }

  /**
   * Returns the URL where to retrieve the access token from.
   */
  @Override
  protected String getAccessTokenUrl(final JsonNode inputOAuthConfiguration) {
    return "https://api.hubapi.com/oauth/v1/token";
  }

}
