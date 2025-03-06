/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.airbyte.oauth.BaseOAuth2Flow;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.http.client.utils.URIBuilder;

/** OneDrive OAuth. */
public class MicrosoftOneDriveOAuthFlow extends BaseOAuth2Flow {

  private static final String fieldName = "tenant_id";

  public MicrosoftOneDriveOAuthFlow(final HttpClient httpClient) {
    super(httpClient);
  }

  @VisibleForTesting
  public MicrosoftOneDriveOAuthFlow(
                                    final HttpClient httpClient,
                                    final Supplier<String> stateSupplier) {
    super(httpClient, stateSupplier);
  }

  private String getScopes() {
    return String.join(" ", "offline_access", "Files.Read.All");
  }

  @Override
  protected String formatConsentUrl(
                                    final UUID definitionId,
                                    final String clientId,
                                    final String redirectUrl,
                                    final JsonNode inputOAuthConfiguration)
      throws IOException {
    final String tenantId = getConfigValueUnsafe(inputOAuthConfiguration, fieldName);

    try {
      return new URIBuilder()
          .setScheme("https")
          .setHost("login.microsoftonline.com")
          .setPath(tenantId + "/oauth2/v2.0/authorize")
          .addParameter("client_id", clientId)
          .addParameter("redirect_uri", redirectUrl)
          .addParameter("state", getState())
          .addParameter("scope", getScopes())
          .addParameter("response_type", "code")
          .build()
          .toString();
    } catch (final URISyntaxException e) {
      throw new IOException("Failed to format Consent URL for OAuth flow", e);
    }
  }

  @Override
  protected Map<String, String> getAccessTokenQueryParameters(
                                                              final String clientId,
                                                              final String clientSecret,
                                                              final String authCode,
                                                              final String redirectUrl) {

    return ImmutableMap.<String, String>builder()
        .put("client_id", clientId)
        .put("code", authCode)
        .put("redirect_uri", redirectUrl)
        .put("client_secret", clientSecret)
        .put("grant_type", "authorization_code")
        .build();
  }

  @Override
  protected String getAccessTokenUrl(final JsonNode inputOAuthConfiguration) {
    final String tenantId = getConfigValueUnsafe(inputOAuthConfiguration, fieldName);
    return "https://login.microsoftonline.com/" + tenantId + "/oauth2/v2.0/token";
  }

}
