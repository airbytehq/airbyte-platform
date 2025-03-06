/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.oauth.BaseOAuth2Flow;
import io.airbyte.protocol.models.OAuthConfigSpecification;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.Base64;
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
  private static final String REVOKE_URL = "https://developer.api.intuit.com/v2/oauth2/tokens/revoke";
  private final Clock clock;

  public QuickbooksOAuthFlow(final HttpClient httpClient) {
    super(httpClient);
    this.clock = Clock.systemUTC();
  }

  public QuickbooksOAuthFlow(final HttpClient httpClient, final Supplier<String> stateSupplier) {
    this(httpClient, stateSupplier, TokenRequestContentType.JSON, Clock.systemUTC());
  }

  public QuickbooksOAuthFlow(final HttpClient httpClient,
                             final Supplier<String> stateSupplier,
                             final Clock clock) {
    this(httpClient, stateSupplier, TokenRequestContentType.JSON, clock);
  }

  public QuickbooksOAuthFlow(final HttpClient httpClient,
                             final Supplier<String> stateSupplier,
                             final TokenRequestContentType tokenReqContentType,
                             final Clock clock) {
    super(httpClient, stateSupplier, tokenReqContentType);
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

  protected String extractRealmIdParameter(final Map<String, Object> queryParams) throws IOException {
    if (queryParams.containsKey("realmId")) {
      return (String) queryParams.get("realmId");
    } else {
      throw new IOException("Undefined 'realmId' from consent redirected url.");
    }
  }

  @Override
  @Deprecated
  public Map<String, Object> completeSourceOAuth(final UUID workspaceId,
                                                 final UUID sourceDefinitionId,
                                                 final Map<String, Object> queryParams,
                                                 final String redirectUrl,
                                                 JsonNode oauthParamConfig)
      throws IOException {
    if (containsIgnoredOAuthError(queryParams)) {
      return buildRequestError(queryParams);
    }
    return formatOAuthOutput(
        oauthParamConfig,
        completeOAuthFlow(
            getClientIdUnsafe(oauthParamConfig),
            getClientSecretUnsafe(oauthParamConfig),
            extractCodeParameter(queryParams),
            extractRealmIdParameter(queryParams),
            redirectUrl,
            Jsons.emptyObject(),
            oauthParamConfig),
        getDefaultOAuthOutputPath());

  }

  @Override
  public Map<String, Object> completeSourceOAuth(final UUID workspaceId,
                                                 final UUID sourceDefinitionId,
                                                 final Map<String, Object> queryParams,
                                                 final String redirectUrl,
                                                 final JsonNode inputOAuthConfiguration,
                                                 final OAuthConfigSpecification oauthConfigSpecification,
                                                 final JsonNode oauthParamConfig)
      throws IOException, JsonValidationException {
    validateInputOAuthConfiguration(oauthConfigSpecification, inputOAuthConfiguration);
    if (containsIgnoredOAuthError(queryParams)) {
      return buildRequestError(queryParams);
    }
    return formatOAuthOutput(
        oauthParamConfig,
        completeOAuthFlow(
            getClientIdUnsafe(oauthParamConfig),
            getClientSecretUnsafe(oauthParamConfig),
            extractCodeParameter(queryParams),
            extractRealmIdParameter(queryParams),
            redirectUrl,
            inputOAuthConfiguration,
            oauthParamConfig),
        oauthConfigSpecification);

  }

  protected Map<String, Object> completeOAuthFlow(final String clientId,
                                                  final String clientSecret,
                                                  final String authCode,
                                                  final String realmId,
                                                  final String redirectUrl,
                                                  final JsonNode inputOAuthConfiguration,
                                                  final JsonNode oauthParamConfig)
      throws IOException {
    final var accessTokenUrl = getAccessTokenUrl(inputOAuthConfiguration);
    final HttpRequest request = HttpRequest.newBuilder()
        .POST(HttpRequest.BodyPublishers
            .ofString(tokenReqContentType.getConverter().apply(getAccessTokenQueryParameters(clientId, clientSecret, authCode, redirectUrl))))
        .uri(URI.create(accessTokenUrl))
        .header("Content-Type", tokenReqContentType.getContentType())
        .header("Accept", "application/json")
        .build();
    // TODO: Handle error response to report better messages
    try {
      final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      return extractOAuthOutput(Jsons.deserialize(response.body()), accessTokenUrl, realmId);
    } catch (final InterruptedException e) {
      throw new IOException("Failed to complete OAuth flow", e);
    }
  }

  @SuppressWarnings("PMD.AvoidDuplicateLiterals")
  protected Map<String, Object> extractOAuthOutput(final JsonNode data, final String accessTokenUrl, final String realmId) throws IOException {
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
    result.put("realm_id", realmId);
    return result;
  }

  @Override
  public void revokeSourceOauth(final UUID workspaceId,
                                final UUID sourceDefinitionId,
                                final JsonNode configuration,
                                final JsonNode oauthParamConfig)
      throws IOException {
    final String clientId = getClientIdUnsafe(oauthParamConfig);
    final String clientSecret = getClientSecretUnsafe(oauthParamConfig);
    final String authorization = Base64.getEncoder()
        .encodeToString((clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));
    final Map<String, String> requestBody = new HashMap<>();
    final JsonNode credentials = configuration.get("credentials");
    requestBody.put("token", getConfigValueUnsafe(credentials, "refresh_token"));

    final HttpRequest request = HttpRequest.newBuilder()
        .POST(HttpRequest.BodyPublishers
            .ofString(tokenReqContentType.getConverter().apply(requestBody)))
        .uri(URI.create(REVOKE_URL))
        .header("Content-Type", tokenReqContentType.getContentType())
        .header("Accept", "application/json")
        .header("Authorization", "Basic " + authorization)
        .build();
    try {
      httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (final InterruptedException e) {
      throw new IOException("Failed to complete revocation request", e);
    }
  }

}
