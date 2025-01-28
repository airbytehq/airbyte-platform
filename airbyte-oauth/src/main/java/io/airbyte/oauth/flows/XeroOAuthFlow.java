/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.oauth.BaseOAuth2Flow;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.http.client.utils.URIBuilder;

/**
 * Following docs from https://developer.xero.com/documentation/guides/oauth2/auth-flow
 */
public class XeroOAuthFlow extends BaseOAuth2Flow {

  private static final String AUTHORIZE_URL = "https://login.xero.com/identity/connect/authorize";
  private static final String ACCESS_TOKEN_URL = "https://identity.xero.com/connect/token";
  private static final List<String> SCOPES = Arrays.asList(
      "accounting.attachments.read",
      "accounting.budgets.read",
      "accounting.contacts.read",
      "accounting.journals.read",
      "accounting.reports.read",
      "accounting.reports.tenninetynine.read",
      "accounting.settings.read",
      "accounting.transactions.read",
      "assets.read",
      "offline_access");
  private final Clock clock;

  public XeroOAuthFlow(final HttpClient httpClient) {
    super(httpClient);
    this.clock = Clock.systemUTC();
  }

  @VisibleForTesting
  public XeroOAuthFlow(final HttpClient httpClient, final Supplier<String> stateSupplier) {
    super(httpClient, stateSupplier);
    this.clock = Clock.systemUTC();
  }

  @Override
  protected String formatConsentUrl(final UUID definitionId,
                                    final String clientId,
                                    final String redirectUrl,
                                    final JsonNode inputOAuthConfiguration)
      throws IOException {
    try {
      return new URIBuilder(AUTHORIZE_URL)
          .addParameter("client_id", clientId)
          .addParameter("redirect_uri", redirectUrl)
          .addParameter("response_type", "code")
          .addParameter("scope", String.join(" ", SCOPES))
          .addParameter("state", getState())
          .build().toString();
    } catch (final URISyntaxException e) {
      throw new IOException("Failed to format Consent URL for OAuth flow", e);
    }
  }

  @Override
  protected Map<String, Object> completeOAuthFlow(final String clientId,
                                                  final String clientSecret,
                                                  final String authCode,
                                                  final String redirectUrl,
                                                  final JsonNode inputOAuthConfiguration,
                                                  final JsonNode oauthParamConfig,
                                                  final String state)
      throws IOException {
    final var accessTokenUrl = getAccessTokenUrl(inputOAuthConfiguration);
    final String authorization = Base64.getEncoder()
        .encodeToString((clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));
    final HttpRequest request = HttpRequest.newBuilder()
        .POST(HttpRequest.BodyPublishers
            .ofString(tokenReqContentType.getConverter().apply(
                getAccessTokenQueryParameters(clientId, clientSecret, authCode, redirectUrl))))
        .uri(URI.create(accessTokenUrl))
        .header("Content-Type", tokenReqContentType.getContentType())
        .header("Authorization", "Basic " + authorization)
        .build();

    try {
      final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      return extractOAuthOutput(Jsons.deserialize(response.body()), accessTokenUrl);
    } catch (final InterruptedException e) {
      throw new IOException("Failed to complete Xero OAuth flow", e);
    }
  }

  @Override
  protected Map<String, String> getAccessTokenQueryParameters(String clientId,
                                                              String clientSecret,
                                                              String authCode,
                                                              String redirectUrl) {
    return ImmutableMap.<String, String>builder()
        // required
        .put("grant_type", "authorization_code")
        .put("code", authCode)
        .put("redirect_uri", redirectUrl)
        .build();
  }

  @Override
  protected String getAccessTokenUrl(final JsonNode inputOAuthConfiguration) {
    return ACCESS_TOKEN_URL;
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
      Instant expiresIn = Instant.now(this.clock).plusSeconds(data.get("expires_in").asInt());
      result.put("token_expiry_date", expiresIn.toString());
    } else {
      throw new IOException(String.format("Missing 'expires_in' in query params from %s", accessTokenUrl));
    }
    return result;
  }

}
