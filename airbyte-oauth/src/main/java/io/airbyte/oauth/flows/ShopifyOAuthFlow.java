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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;

/**
 * Shopify Oauth.
 */
public class ShopifyOAuthFlow extends BaseOAuth2Flow {

  private static final String shop = "shop";

  public ShopifyOAuthFlow(final HttpClient httpClient) {
    super(httpClient);
  }

  @Override
  protected String formatConsentUrl(final UUID definitionId,
                                    final String clientId,
                                    final String redirectUrl,
                                    final JsonNode inputOAuthConfiguration)
      throws IOException {
    /*
     * Build the URL that leads to the Shopify Marketplace showing the `Airbyte` application to install.
     */
    final URIBuilder builder = new URIBuilder()
        .setScheme("https")
        .setHost("apps.shopify.com")
        .setPath("airbyte");

    try {
      return builder.build().toString();
    } catch (URISyntaxException e) {
      throw new IOException("Failed to format Consent URL for OAuth flow", e);
    }
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
            extractShopParameter(queryParams),
            redirectUrl,
            inputOAuthConfiguration,
            oauthParamConfig),
        oauthConfigSpecification);
  }

  protected Map<String, Object> completeOAuthFlow(final String clientId,
                                                  final String clientSecret,
                                                  final String authCode,
                                                  final String shopName,
                                                  final String redirectUrl,
                                                  final JsonNode inputOAuthConfiguration,
                                                  final JsonNode oauthParamConfig)
      throws IOException {
    final var accessTokenUrl = formatAccessTokenUrl(shopName);
    final HttpRequest request = HttpRequest.newBuilder()
        .POST(HttpRequest.BodyPublishers
            .ofString(tokenReqContentType.getConverter().apply(
                getAccessTokenQueryParameters(clientId, clientSecret, authCode, redirectUrl))))
        .uri(URI.create(accessTokenUrl))
        .header("Content-Type", tokenReqContentType.getContentType())
        .header("Accept", "application/json")
        .build();
    try {
      final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      return extractOAuthOutput(Jsons.deserialize(response.body()), accessTokenUrl, shopName);
    } catch (final InterruptedException e) {
      throw new IOException("Failed to complete OAuth flow", e);
    }
  }

  @Override
  protected Map<String, String> getAccessTokenQueryParameters(String clientId,
                                                              String clientSecret,
                                                              String authCode,
                                                              String redirectUrl) {
    return ImmutableMap.<String, String>builder()
        .put("client_id", clientId)
        .put("client_secret", clientSecret)
        .put("code", authCode)
        .build();
  }

  @Override
  protected String getAccessTokenUrl(final JsonNode inputOAuthConfiguration) {
    return StringUtils.EMPTY;
  }

  private String formatAccessTokenUrl(final String shopName) {
    // building the access_token_url with the shop name
    return "https://" + shopName + "/admin/oauth/access_token";
  }

  private String extractShopParameter(final Map<String, Object> queryParams) throws IOException {
    if (queryParams.containsKey(shop)) {
      return (String) queryParams.get(shop);
    } else {
      throw new IOException("Undefined 'shop' from consent redirected url.");
    }
  }

  @SuppressWarnings("PMD.AvoidDuplicateLiterals")
  protected Map<String, Object> extractOAuthOutput(final JsonNode data, final String accessTokenUrl, final String shopName) throws IOException {
    final Map<String, Object> result = new HashMap<>();
    // getting out access_token
    if (data.has("access_token")) {
      result.put("access_token", data.get("access_token").asText());
    } else {
      throw new IOException(String.format("Missing 'access_token' in query params from %s", accessTokenUrl));
    }
    result.put("shop", shopName);
    return result;
  }

}
