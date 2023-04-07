/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.oauth.BaseOAuth2Flow;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.http.client.utils.URIBuilder;

/**
 * Quickbooks OAuth.
 */
public class QuickbooksOAuthFlow extends BaseOAuth2Flow {

  private static final String CONSENT_URL = "https://appcenter.intuit.com/app/connect/oauth2";
  private static final String TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer";
  private final Clock clock;

  public QuickbooksOAuthFlow(final ConfigRepository configRepository, final HttpClient httpClient) {
    super(configRepository, httpClient);
    this.clock = Clock.systemUTC();
  }

  public QuickbooksOAuthFlow(final ConfigRepository configRepository, final HttpClient httpClient, final Supplier<String> stateSupplier) {
    this(configRepository, httpClient, stateSupplier, Clock.systemUTC());
  }

  public QuickbooksOAuthFlow(final ConfigRepository configRepository,
                             final HttpClient httpClient,
                             final Supplier<String> stateSupplier,
                             final Clock clock) {
    super(configRepository, httpClient, stateSupplier);
    this.clock = clock;
  }

  public String getScopes() {
    return "com.intuit.quickbooks.accounting";
  }

  @Override
  protected String formatConsentUrl(final UUID definitionId,
                                    final String clientId,
                                    final String redirectUrl,
                                    final JsonNode inputOAuthConfiguration)
      throws IOException {
    try {

      return (new URIBuilder(CONSENT_URL)
          .addParameter("client_id", clientId)
          .addParameter("scope", getScopes())
          .addParameter("redirect_uri", redirectUrl)
          .addParameter("response_type", "code")
          .addParameter("state", getState())
          .build()).toString();

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
        .put("redirect_uri", redirectUrl)
        .put("grant_type", "authorization_code")
        .put("code", authCode)
        .put("client_id", clientId)
        .put("client_secret", clientSecret)
        .build();
  }

  /**
   * Returns the URL where to retrieve the access token from.
   */
  @Override
  protected String getAccessTokenUrl(final JsonNode inputOAuthConfiguration) {
    return TOKEN_URL;
  }

  @Override
  protected Map<String, Object> extractOAuthOutput(final JsonNode data, final String accessTokenUrl) throws IOException {
    final Map<String, Object> result = new HashMap<>();
    if (data.has("refresh_token")) {
      result.put("refresh_token", data.get("refresh_token").asText());
    } else {
      throw new IOException(String.format("Missing 'refresh_token' in query params from %s", accessTokenUrl));
    }
    if (data.has("access_token")) {
      result.put("access_token", data.get("access_token").asText());
    } else {
      throw new IOException(String.format("Missing 'access_token' in query params from %s", accessTokenUrl));
    }
    if (data.has("expires_in")) {
      final Instant expiresIn = Instant.now(this.clock).plusSeconds(data.get("expires_in").asInt());
      result.put("token_expiry_date", expiresIn.toString());
    } else {
      throw new IOException(String.format("Missing 'expires_in' in query params from %s", accessTokenUrl));
    }
    return result;
  }

}
