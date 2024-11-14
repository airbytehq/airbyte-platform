/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.declarative;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.oauth.BaseOAuth2Flow;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.http.client.utils.URIBuilder;

public class DeclarativeOAuthFlow extends BaseOAuth2Flow {

  public final DeclarativeOAuthSpecHandler specHandler = new DeclarativeOAuthSpecHandler();

  public DeclarativeOAuthFlow(final HttpClient httpClient) {
    super(httpClient);
  }

  @VisibleForTesting
  public DeclarativeOAuthFlow(final HttpClient httpClient, final Supplier<String> stateSupplier) {
    super(httpClient, stateSupplier);
  }

  /**
   * Overrides the getState method to provide a customizable STATE parameter.
   *
   * @param inputOAuthConfiguration the JSON node containing OAuth configuration details.
   * @return a customizable STATE parameter as a String.
   */
  @Override
  protected String getState(final JsonNode inputOAuthConfiguration) {
    final JsonNode state = inputOAuthConfiguration.path(DeclarativeOAuthSpecHandler.STATE_PARAM_KEY);
    return (state.isMissingNode()) ? getState() : specHandler.getConfigurableState(state);
  }

  /**
   * IMPORTANT: DO NOT MODIFY!
   *
   * Formats the consent URL for the OAuth flow by interpolating values from the input configuration.
   *
   * @param definitionId The UUID of the definition.
   * @param clientId The client ID for the OAuth application.
   * @param redirectUrl The URL to which the OAuth provider will redirect after authorization.
   * @param inputOAuthConfiguration The JSON node containing the OAuth configuration.
   * @return The formatted consent URL as a string.
   * @throws IOException If there is an error in formatting the consent URL or if the URL is invalid.
   */
  @Override
  protected String formatConsentUrl(final UUID definitionId,
                                    final String clientId,
                                    final String redirectUrl,
                                    final JsonNode inputOAuthConfiguration)
      throws IOException {

    final String consentUrlRendered = specHandler.renderStringTemplate(
        specHandler.getConsentUrlTemplateValues(
            inputOAuthConfiguration,
            clientId,
            redirectUrl,
            getState(inputOAuthConfiguration)),
        getConfigValueUnsafe(inputOAuthConfiguration, DeclarativeOAuthSpecHandler.CONSENT_URL));

    try {
      return new URIBuilder(consentUrlRendered).build().toString();
    } catch (final URISyntaxException e) {
      throw new IOException("Failed to format Consent URL for OAuth flow.", e);
    }
  }

  /**
   * IMPORTANT: DO NOT MODIFY!
   *
   * This is the override for the base `getAccessTokenQueryParameters`. For the Declarative way of how
   * the `access_token_url` is constructed, we use the `${placeHolders}` to provide the in-place
   * interpolation, instead of having the complete `HashMap`.
   *
   * @return An empty HashMap.
   */
  @Override
  protected Map<String, String> getAccessTokenQueryParameters(final String clientId,
                                                              final String clientSecret,
                                                              final String authCode,
                                                              final String redirectUrl,
                                                              final JsonNode inputOAuthConfiguration) {
    return new HashMap<>();
  }

  /**
   * IMPORTANT: DO NOT MODIFY!
   *
   * Retrieves the access token URL from the provided OAuth configuration.
   *
   * @param inputOAuthConfiguration the JSON node containing the OAuth configuration.
   * @return the access token URL.
   */
  @Override
  protected String getAccessTokenUrl(final JsonNode inputOAuthConfiguration) {
    return getConfigValueUnsafe(inputOAuthConfiguration, DeclarativeOAuthSpecHandler.ACCESS_TOKEN_URL);
  }

  /**
   * Overrides the method to get the complete OAuth flow request headers.
   *
   * @param clientId The client ID for the OAuth flow.
   * @param clientSecret The client secret for the OAuth flow.
   * @param authCode The authorization code received from the OAuth provider.
   * @param redirectUrl The redirect URL used in the OAuth flow.
   * @param inputOAuthConfiguration The JSON node containing the OAuth configuration.
   * @return A map containing the complete OAuth flow request headers.
   * @throws IOException If an I/O error occurs during the process.
   */
  @Override
  protected Map<String, String> getCompleteOAuthFlowRequestHeaders(final String clientId,
                                                                   final String clientSecret,
                                                                   final String authCode,
                                                                   final String redirectUrl,
                                                                   final JsonNode inputOAuthConfiguration)
      throws IOException {

    final Map<String, String> renderedCompleteOAuthHeaders =
        specHandler.renderCompleteOAuthHeaders(
            specHandler.getCompleteOAuthHeadersTemplateValues(
                inputOAuthConfiguration,
                clientId,
                clientSecret,
                authCode,
                redirectUrl),
            inputOAuthConfiguration);

    return (!renderedCompleteOAuthHeaders.isEmpty())
        ? renderedCompleteOAuthHeaders
        : super.getCompleteOAuthFlowRequestHeaders(
            clientId,
            clientSecret,
            authCode,
            redirectUrl,
            inputOAuthConfiguration);
  }

  /**
   * Formats the access token URL with the provided parameters.
   *
   * @param accessTokenUrl The base URL for obtaining the access token.
   * @param clientId The client ID for the OAuth application.
   * @param clientSecret The client secret for the OAuth application.
   * @param authCode The authorization code received from the OAuth provider.
   * @param redirectUrl The URL to which the OAuth provider will redirect after authorization.
   * @param inputOAuthConfiguration Additional OAuth configuration parameters.
   * @return The formatted access token URL.
   */
  @Override
  protected String formatAccessTokenUrl(final String accessTokenUrl,
                                        final String clientId,
                                        final String clientSecret,
                                        final String authCode,
                                        final String redirectUrl,
                                        final JsonNode inputOAuthConfiguration,
                                        final String state)
      throws IOException {

    final String accessTokenUrlRendered = specHandler.renderStringTemplate(
        specHandler.getAccessTokenUrlTemplateValues(
            inputOAuthConfiguration,
            clientId,
            clientSecret,
            authCode,
            redirectUrl,
            state),
        accessTokenUrl);

    try {
      return new URIBuilder(accessTokenUrlRendered).build().toString();
    } catch (final URISyntaxException e) {
      throw new IOException("Failed to format AccessToken URL for OAuth flow.", e);
    }
  }

  /**
   * IMPORTANT: DO NOT MODIFY!
   *
   * Extracts the OAuth output from the provided data and configuration.
   *
   * @param data The JSON node containing the data from which to extract the OAuth output.
   * @param accessTokenUrl The URL used to obtain the access token.
   * @param inputOAuthConfiguration The JSON node containing the input OAuth configuration.
   * @return A map containing the extracted OAuth output.
   * @throws IOException If an I/O error occurs during the extraction process.
   */
  @Override
  protected Map<String, Object> extractOAuthOutput(final JsonNode data,
                                                   final String accessTokenUrl,
                                                   final JsonNode inputOAuthConfiguration)
      throws IOException {
    return specHandler.processOAuthOutput(inputOAuthConfiguration, data, accessTokenUrl);
  }

  /**
   * This function should parse and extract the state from these query parameters in order to continue
   * the OAuth Flow.
   */
  @Override
  protected String extractStateParameter(final Map<String, Object> queryParams,
                                         final JsonNode inputOAuthConfiguration)
      throws IOException {
    // get the state key name with respect to userConfig input
    final String stateKey = specHandler.getStateKey(inputOAuthConfiguration);
    if (queryParams.containsKey(stateKey)) {
      return (String) queryParams.get(stateKey);
    } else {
      final String errorMsg = String.format("Undefined `state_key`: '%s' from `redirect_uri`. Keys available: %s", stateKey, queryParams.keySet());
      throw new IOException(errorMsg);
    }
  }

}
