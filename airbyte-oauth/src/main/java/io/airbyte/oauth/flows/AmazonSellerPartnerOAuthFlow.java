/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.oauth.BaseOAuth2Flow;
import io.airbyte.protocol.models.OAuthConfigSpecification;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.http.client.utils.URIBuilder;

/**
 * Amazon Sellers OAuth.
 */
public class AmazonSellerPartnerOAuthFlow extends BaseOAuth2Flow {

  private static final String AUTHORIZE_URL = "https://sellercentral.amazon.com/apps/authorize/consent";
  private static final String ACCESS_TOKEN_URL = "https://api.amazon.com/auth/o2/token";

  @Override
  protected String getClientIdUnsafe(final JsonNode oauthConfig) {
    return getConfigValueUnsafe(oauthConfig, "lwa_app_id");
  }

  @Override
  protected String getClientSecretUnsafe(final JsonNode oauthConfig) {
    return getConfigValueUnsafe(oauthConfig, "lwa_client_secret");
  }

  public AmazonSellerPartnerOAuthFlow(final HttpClient httpClient) {
    super(httpClient);
  }

  public AmazonSellerPartnerOAuthFlow(final HttpClient httpClient, final Supplier<String> stateSupplier) {
    super(httpClient, stateSupplier);
  }

  /*
   * Overriden default method to provide possibility to retrieve `app_id` from
   * `sourceOAuthParamConfig` bypassing `formatConsentUrl()` method.
   */
  @Override
  public String getSourceConsentUrl(final UUID workspaceId,
                                    final UUID sourceDefinitionId,
                                    final String redirectUrl,
                                    final JsonNode inputOAuthConfiguration,
                                    final OAuthConfigSpecification oauthConfigSpecification,
                                    final JsonNode sourceOAuthParamConfig)
      throws IOException, ConfigNotFoundException, JsonValidationException {
    validateInputOAuthConfiguration(oauthConfigSpecification, inputOAuthConfiguration);

    if (sourceOAuthParamConfig == null) {
      throw new ConfigNotFoundException(ConfigSchema.SOURCE_OAUTH_PARAM, "Undefined OAuth Parameter.");
    }

    try {
      return new URIBuilder(AUTHORIZE_URL)
          /*
           * Airbyte Amazon Seller Partner `application_id`, to provide native OAuth integration for
           * 3rd-parties.
           */
          // get the `app_id` parameter from instance-wide params
          .addParameter("application_id", getConfigValueUnsafe(sourceOAuthParamConfig, "app_id"))
          .addParameter("redirect_uri", redirectUrl)
          .addParameter("state", getState())
          /*
           * Use `version=beta` for OAuth tests only, or when the OAuth App is in `draft` status
           * https://developer-docs.amazon.com/amazon-shipping/docs/authorizing-selling-partner-api-
           * applications#constructing-an-oauth-authorization-uri .addParameter("version", "beta")
           */
          .build().toString();
    } catch (final URISyntaxException e) {
      throw new IOException("Failed to format Consent URL for OAuth flow", e);
    }

  }

  @Override
  protected String extractCodeParameter(final Map<String, Object> queryParams) throws IOException {
    if (queryParams.containsKey("spapi_oauth_code")) {
      return (String) queryParams.get("spapi_oauth_code");
    } else {
      throw new IOException("Undefined 'spapi_oauth_code' from consent redirected url.");
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

  @Override
  public List<String> getDefaultOAuthOutputPath() {
    return List.of();
  }

  /*
   * Should be overriden to satisfy the BaseOAuth2Flow abstraction requirements
   */
  @Override
  protected String formatConsentUrl(final UUID definitionId,
                                    final String clientId,
                                    final String redirectUrl,
                                    final JsonNode inputOAuthConfiguration)
      throws IOException {
    return "";
  }

}
