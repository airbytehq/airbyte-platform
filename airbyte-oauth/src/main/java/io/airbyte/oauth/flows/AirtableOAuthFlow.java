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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.http.client.utils.URIBuilder;

/**
 * Following docs from
 * https://airtable.com/developers/web/api/oauth-reference#authorization-request-query
 */
public class AirtableOAuthFlow extends BaseOAuth2Flow {

  private static final String ACCESS_TOKEN_URL = "https://airtable.com/oauth2/v1/token";
  private final Clock clock;
  private final SecureRandom secureRandom;
  private static final List<String> SCOPES = Arrays.asList(
      "data.records:read",
      "data.recordComments:read",
      "schema.bases:read");

  public String getScopes() {
    // More info and additional scopes could be found here:
    // https://airtable.com/developers/web/api/scopes
    // should be space-delimitered
    return String.join(" ", SCOPES);
  }

  /**
   * Must be a cryptographically generated string; 43-128 characters long
   * https://airtable.com/developers/web/api/oauth-reference#authorization-parameter-rules
   */
  public String getCodeVerifier() {
    String allowedCharacters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_";
    int length = this.secureRandom.nextInt((128 - 43) + 1) + 43;
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      int randomIndex = this.secureRandom.nextInt(allowedCharacters.length());
      char randomChar = allowedCharacters.charAt(randomIndex);
      sb.append(randomChar);
    }
    return sb.toString();
  }

  /**
   * Base64 url encoding of the sha256 hash of code_verifier
   * https://airtable.com/developers/web/api/oauth-reference#authorization-parameter-rules
   */
  public String getCodeChallenge(String codeVerifier) throws IOException {
    try {
      MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
      messageDigest.update(codeVerifier.getBytes(StandardCharsets.UTF_8));
      byte[] codeChallengeBytes = messageDigest.digest();
      return Base64.getUrlEncoder().withoutPadding().encodeToString(codeChallengeBytes);
    } catch (NoSuchAlgorithmException e) {
      throw new IOException("Failed to get code_challenge for OAuth flow", e);
    }
  }

  public AirtableOAuthFlow(final HttpClient httpClient) {
    super(httpClient);
    this.clock = Clock.systemUTC();
    this.secureRandom = new SecureRandom();
  }

  @Override
  protected String formatConsentUrl(final UUID definitionId,
                                    final String clientId,
                                    final String redirectUrl,
                                    final JsonNode inputOAuthConfiguration)
      throws IOException {

    final String codeVerifier = getCodeVerifier();

    final URIBuilder builder = new URIBuilder()
        .setScheme("https")
        .setHost("airtable.com")
        .setPath("oauth2/v1/authorize")
        // required
        .addParameter("redirect_uri", redirectUrl)
        .addParameter("client_id", clientId)
        .addParameter("response_type", "code")
        .addParameter("scope", getScopes())
        .addParameter("code_challenge", getCodeChallenge(codeVerifier))
        .addParameter("code_challenge_method", "S256")
        .addParameter("state", codeVerifier);

    try {
      return builder.build().toString();
    } catch (final URISyntaxException e) {
      throw new IOException("Failed to format Consent URL for OAuth flow", e);
    }
  }

  @Override
  protected String getAccessTokenUrl(final JsonNode inputOAuthConfiguration) {
    return ACCESS_TOKEN_URL;
  }

  protected Map<String, String> getAccessTokenQueryParameters(String clientId,
                                                              String clientSecret,
                                                              String authCode,
                                                              String state,
                                                              String redirectUrl) {
    return ImmutableMap.<String, String>builder()
        // required
        .put("code", authCode)
        .put("redirect_uri", redirectUrl)
        .put("grant_type", "authorization_code")
        .put("client_id", clientId)
        .put("code_verifier", state)
        .put("code_challenge_method", "S256")
        .build();
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
            extractStateParameter(queryParams),
            redirectUrl,
            inputOAuthConfiguration,
            oauthParamConfig),
        oauthConfigSpecification);

  }

  protected Map<String, Object> completeOAuthFlow(final String clientId,
                                                  final String clientSecret,
                                                  final String authCode,
                                                  final String state,
                                                  final String redirectUrl,
                                                  final JsonNode inputOAuthConfiguration,
                                                  final JsonNode oauthParamConfig)
      throws IOException {
    final var accessTokenUrl = getAccessTokenUrl(inputOAuthConfiguration);
    final byte[] authorization = Base64.getEncoder()
        .encode((clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));
    final HttpRequest request = HttpRequest.newBuilder()
        .POST(HttpRequest.BodyPublishers
            .ofString(tokenReqContentType.getConverter().apply(
                getAccessTokenQueryParameters(clientId, clientSecret, authCode, state, redirectUrl))))
        .uri(URI.create(accessTokenUrl))
        .header("Content-Type", tokenReqContentType.getContentType())
        .header("Authorization", "Basic " + new String(authorization, StandardCharsets.UTF_8))
        .build();
    try {
      final HttpResponse<String> response = httpClient.send(request,
          HttpResponse.BodyHandlers.ofString());

      return extractOAuthOutput(Jsons.deserialize(response.body()), accessTokenUrl);
    } catch (final InterruptedException e) {
      throw new IOException("Failed to complete OAuth flow", e);
    }
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

  /**
   * This function should parse and extract the state from these query parameters in order to continue
   * the OAuth Flow.
   */
  protected String extractStateParameter(final Map<String, Object> queryParams) throws IOException {
    if (queryParams.containsKey("state")) {
      return (String) queryParams.get("state");
    } else {
      throw new IOException("Undefined 'state' from consent redirected url.");
    }
  }

}
