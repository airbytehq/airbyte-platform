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
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.http.client.utils.URIBuilder;

/**
 * Following docs from https://docs.gitlab.com/ee/api/oauth2.html#authorization-code-flow.
 */
public class GitlabOAuthFlow extends BaseOAuth2Flow {

  private static final String ACCESS_TOKEN_URL = "https://%s/oauth/token";
  private static final String DEFAULT_GITLAB_DOMAIN = "gitlab.com";

  public GitlabOAuthFlow(final HttpClient httpClient) {
    super(httpClient);
  }

  public GitlabOAuthFlow(final HttpClient httpClient, final Supplier<String> stateSupplier) {
    super(httpClient, stateSupplier);
  }

  protected static String getDomain(JsonNode inputOAuthConfiguration) {
    String stringURL = DEFAULT_GITLAB_DOMAIN;
    if (inputOAuthConfiguration.has("domain")) {
      String url = inputOAuthConfiguration.get("domain").asText();
      if (!url.isEmpty()) {
        stringURL = url;
      }
    }
    // this could be `https://gitlab.com` or `gitlab.com`
    // because the connector supports storing hostname with and without schema
    final String[] parts = stringURL.split("//");
    return parts[parts.length - 1];
  }

  @Override
  protected String formatConsentUrl(final UUID definitionId, final String clientId, final String redirectUrl, final JsonNode inputOAuthConfiguration)
      throws IOException {
    final URIBuilder builder = new URIBuilder()
        .setScheme("https")
        .setHost(getDomain(inputOAuthConfiguration))
        .setPath("oauth/authorize")
        .addParameter("client_id", clientId)
        .addParameter("redirect_uri", redirectUrl)
        .addParameter("state", getState())
        .addParameter("response_type", "code")
        .addParameter("scope", "read_api");
    try {
      return builder.build().toString();
    } catch (URISyntaxException e) {
      throw new IOException("Failed to format Consent URL for OAuth flow", e);
    }
  }

  @Override
  protected String getAccessTokenUrl(final JsonNode inputOAuthConfiguration) {
    final var domain = getDomain(inputOAuthConfiguration);
    return String.format(ACCESS_TOKEN_URL, domain);
  }

  @Override
  protected Map<String, String> getAccessTokenQueryParameters(final String clientId,
                                                              final String clientSecret,
                                                              final String authCode,
                                                              final String redirectUrl) {
    return ImmutableMap.<String, String>builder()
        .put("client_id", clientId)
        .put("client_secret", clientSecret)
        .put("code", authCode)
        .put("grant_type", "authorization_code")
        .put("redirect_uri", redirectUrl)
        .build();
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
      final Instant expiresIn = Instant.ofEpochSecond(data.get("created_at").asInt()).plusSeconds(data.get("expires_in").asInt());
      result.put("token_expiry_date", expiresIn.toString());
    } else {
      throw new IOException(String.format("Missing 'expires_in' in query params from %s", accessTokenUrl));
    }
    return result;
  }

  @Override
  @Deprecated
  public Map<String, Object> completeSourceOAuth(final UUID workspaceId,
                                                 final UUID sourceDefinitionId,
                                                 final Map<String, Object> queryParams,
                                                 final String redirectUrl,
                                                 JsonNode oauthParamConfig)
      throws IOException {
    throw new IOException("Deprecated API not supported by this connector");
  }

}
