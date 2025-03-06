/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.airbyte.commons.json.Jsons;
import io.airbyte.oauth.BaseOAuth2Flow;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.http.client.utils.URIBuilder;

/**
 * Following docs from
 * https://ads.tiktok.com/marketing_api/docs?id=1701890912382977&is_new_connect=0&is_new_user=0&rid=sta6fe2yww.
 */
public class TikTokMarketingOAuthFlow extends BaseOAuth2Flow {

  private static final String ACCESS_TOKEN_URL = "https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token/";

  @Override
  protected String getClientIdUnsafe(final JsonNode oauthConfig) {
    return getConfigValueUnsafe(oauthConfig, "app_id");
  }

  @Override
  protected String getClientSecretUnsafe(final JsonNode oauthConfig) {
    return getConfigValueUnsafe(oauthConfig, "secret");
  }

  public TikTokMarketingOAuthFlow(final HttpClient httpClient) {
    super(httpClient);
  }

  @VisibleForTesting
  public TikTokMarketingOAuthFlow(final HttpClient httpClient, final Supplier<String> stateSupplier) {
    super(httpClient, stateSupplier, TokenRequestContentType.JSON);
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
    final HttpRequest request = HttpRequest.newBuilder()
        .POST(HttpRequest.BodyPublishers
            .ofString(toJson(getAccessTokenQueryParameters(clientId, clientSecret, authCode, redirectUrl))))
        .uri(URI.create(accessTokenUrl))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .build();
    // TODO: Handle error response to report better messages
    try {
      final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      return extractOAuthOutput(Jsons.deserialize(response.body()), accessTokenUrl);
    } catch (final InterruptedException e) {
      throw new IOException("Failed to complete OAuth flow", e);
    }
  }

  @Override
  protected String formatConsentUrl(final UUID definitionId,
                                    final String appId,
                                    final String redirectUrl,
                                    final JsonNode inputOAuthConfiguration)
      throws IOException {

    final URIBuilder builder = new URIBuilder()
        .setScheme("https")
        .setHost("ads.tiktok.com")
        .setPath("marketing_api/auth")
        // required
        .addParameter("app_id", appId)
        .addParameter("redirect_uri", redirectUrl)
        .addParameter("state", getState());

    try {
      return builder.build().toString();
    } catch (final URISyntaxException e) {
      throw new IOException("Failed to format Consent URL for OAuth flow", e);
    }
  }

  @Override
  protected Map<String, String> getAccessTokenQueryParameters(final String appId,
                                                              final String secret,
                                                              final String authCode,
                                                              final String redirectUrl) {
    return ImmutableMap.<String, String>builder()
        // required
        .put("auth_code", authCode)
        .put("app_id", appId)
        .put("secret", secret)
        .build();
  }

  @Override
  protected String getAccessTokenUrl(final JsonNode inputOAuthConfiguration) {
    return ACCESS_TOKEN_URL;
  }

  @Override
  protected String extractCodeParameter(final Map<String, Object> queryParams) throws IOException {
    if (queryParams.containsKey("auth_code")) {
      return (String) queryParams.get("auth_code");
    } else if (queryParams.containsKey("code")) {
      return (String) queryParams.get("code");
    } else {
      throw new IOException("Undefined 'auth_code'/'code' from consent redirected url.");
    }
  }

  @Override
  protected Map<String, Object> extractOAuthOutput(final JsonNode data, final String accessTokenUrl) throws IOException {
    final Map<String, Object> result = new HashMap<>();
    // getting out access_token
    if ((data.has("data")) && (data.get("data").has("access_token"))) {
      result.put("access_token", data.get("data").get("access_token").asText());
    } else {
      throw new IOException(String.format("Missing 'access_token' in query params from %s, data: %s", accessTokenUrl, data.toString()));
    }
    return result;
  }

  protected static String toJson(final Map<String, String> body) {
    final Gson gson = new Gson();
    final Type gsonType = new TypeToken<Map<String, String>>() {}.getType();
    return gson.toJson(body, gsonType);
  }

}
