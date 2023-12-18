/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
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
 * Amazon Ads OAuth.
 */
public class AmazonAdsOAuthFlow extends BaseOAuth2Flow {

  private static final String AUTHORIZE_URL = "https://www.amazon.com/ap/oa";
  private static final String AUTHORIZE_EU_URL = "https://eu.account.amazon.com/ap/oa";
  private static final String AUTHORIZE_FE_URL = "https://apac.account.amazon.com/ap/oa";
  private static final String ACCESS_TOKEN_URL = "https://api.amazon.com/auth/o2/token";

  private static final Map<String, String> AUTHORIZE_URL_MAP = ImmutableMap.of(
      "NA", AUTHORIZE_URL,
      "EU", AUTHORIZE_EU_URL,
      "FE", AUTHORIZE_FE_URL);

  public AmazonAdsOAuthFlow(final HttpClient httpClient) {
    super(httpClient);
  }

  public AmazonAdsOAuthFlow(final HttpClient httpClient, final Supplier<String> stateSupplier) {
    super(httpClient, stateSupplier);
  }

  /**
   * Depending on the OAuth flow implementation, the URL to grant user's consent may differ,
   * especially in the query parameters to be provided. This function should generate such consent URL
   * accordingly.
   *
   * @param definitionId The configured definition ID of this client
   * @param clientId The configured client ID
   * @param redirectUrl the redirect URL
   */
  @Override
  protected String formatConsentUrl(final UUID definitionId,
                                    final String clientId,
                                    final String redirectUrl,
                                    final JsonNode inputOAuthConfiguration)
      throws IOException {
    try {
      String authorizeUrl = inputOAuthConfiguration.has("region")
          ? AUTHORIZE_URL_MAP.get(inputOAuthConfiguration.get("region").asText())
          : AUTHORIZE_URL;
      return new URIBuilder(authorizeUrl)
          .addParameter("client_id", clientId)
          .addParameter("scope", "advertising::campaign_management")
          .addParameter("response_type", "code")
          .addParameter("redirect_uri", redirectUrl)
          .addParameter("state", getState())
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

  /**
   * Returns the URL where to retrieve the access token from.
   *
   */
  @Override
  protected String getAccessTokenUrl(final JsonNode inputOAuthConfiguration) {
    return ACCESS_TOKEN_URL;
  }

}
