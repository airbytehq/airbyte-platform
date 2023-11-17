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

  private static final String ACCESS_TOKEN_URL = "https://api.amazon.com/auth/o2/token";
  private static final String sellerEuropeUrl = "https://sellercentral-europe.amazon.com";
  private static Map<String, String> vendorCentralUrls = ImmutableMap.<String, String>builder()
      .put("CA", "https://vendorcentral.amazon.ca")
      .put("US", "https://vendorcentral.amazon.com")
      .put("DE", "https://vendorcentral.amazon.de")
      .put("MX", "https://vendorcentral.amazon.com.mx")
      .put("JP", "https://vendorcentral.amazon.co.jp")
      .put("IT", "https://vendorcentral.amazon.it")
      .put("AU", "https://vendorcentral.amazon.com.au")
      .put("BR", "https://vendorcentral.amazon.com.br")
      .put("BE", "https://vendorcentral.amazon.com.be")
      .put("ES", "https://vendorcentral.amazon.es")
      .put("UK", "https://vendorcentral.amazon.co.uk")
      .put("GB", "https://vendorcentral.amazon.co.uk")
      .put("NL", "https://vendorcentral.amazon.nl")
      .put("PL", "https://vendorcentral.amazon.pl")
      .put("FR", "https://vendorcentral.amazon.fr")
      .put("IN", "https://www.vendorcentral.in")
      .put("SE", "https://vendorcentral.amazon.se")
      .put("SG", "https://vendorcentral.amazon.com.sg")
      .put("AE", "https://vendorcentral.amazon.me")
      .put("TR", "https://vendorcentral.amazon.com.tr")
      .put("SA", "https://vendorcentral.amazon.me")
      .put("EG", "https://vendorcentral.amazon.me")
      .put("ZA", "https://vendorcentral.amazon.co.za")
      .build();

  private static Map<String, String> sellerCentralUrls = ImmutableMap.<String, String>builder()
      .put("CA", "https://sellercentral.amazon.ca")
      .put("US", "https://sellercentral.amazon.com")
      .put("MX", "https://sellercentral.amazon.com.mx")
      .put("BR", "https://sellercentral.amazon.com.br")
      .put("ES", sellerEuropeUrl)
      .put("UK", sellerEuropeUrl)
      .put("GB", sellerEuropeUrl)
      .put("FR", sellerEuropeUrl)
      .put("NL", "https://sellercentral.amazon.nl")
      .put("DE", sellerEuropeUrl)
      .put("IT", sellerEuropeUrl)
      .put("SE", "https://sellercentral.amazon.se")
      .put("ZA", "https://sellercentral.amazon.co.za")
      .put("PL", "https://sellercentral.amazon.pl")
      .put("EG", "https://sellercentral.amazon.eg")
      .put("TR", "https://sellercentral.amazon.com.tr")
      .put("SA", "https://sellercentral.amazon.sa")
      .put("AE", "https://sellercentral.amazon.ae")
      .put("IN", "https://sellercentral.amazon.in")
      .put("BE", "https://sellercentral.amazon.com.be")
      .put("SG", "https://sellercentral.amazon.sg")
      .put("AU", "https://sellercentral.amazon.com.au")
      .put("JP", "https://sellercentral.amazon.co.jp")
      .build();

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
      final String accountType = getConfigValueUnsafe(inputOAuthConfiguration, "account_type");
      final String region = getConfigValueUnsafe(inputOAuthConfiguration, "region");
      String consentUrl;

      if ("Vendor".equals(accountType)) {
        consentUrl = vendorCentralUrls.get(region);
      } else {
        consentUrl = sellerCentralUrls.get(region);
      }

      return new URIBuilder(consentUrl + "/apps/authorize/consent")
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
