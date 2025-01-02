/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.airbyte.api.problems.model.generated.ProblemResourceData;
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ConfigSchema;
import io.airbyte.protocol.models.OAuthConfigSpecification;
import io.airbyte.validation.json.JsonSchemaValidator;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * Abstract Class factoring common behavior for oAuth 2.0 flow implementations.
 */
public abstract class BaseOAuth2Flow extends BaseOAuthFlow {

  /**
   * Simple enum of content type strings and their respective encoding functions used for POSTing the
   * access token request.
   */
  public enum TokenRequestContentType {

    URL_ENCODED("application/x-www-form-urlencoded", BaseOAuth2Flow::toUrlEncodedString),
    JSON("application/json", BaseOAuth2Flow::toJson);

    String contentType;
    Function<Map<String, String>, String> converter;

    /**
     * Get HTTP content type.
     *
     * @return content type
     */
    public String getContentType() {
      return contentType;
    }

    /**
     * Get converter to json.
     *
     * @return converter function
     */
    public Function<Map<String, String>, String> getConverter() {
      return converter;
    }

    TokenRequestContentType(final String contentType, final Function<Map<String, String>, String> converter) {
      this.contentType = contentType;
      this.converter = converter;
    }

  }

  protected final HttpClient httpClient;
  protected final TokenRequestContentType tokenReqContentType;
  private final Supplier<String> stateSupplier;

  // possible errors enumerated @
  // https://www.oauth.com/oauth2-servers/server-side-apps/possible-errors/
  private final List<String> ignoredOauthErrors = List.of("access_denied");

  public BaseOAuth2Flow(final HttpClient httpClient) {
    this(httpClient, BaseOAuth2Flow::generateRandomState);
  }

  public BaseOAuth2Flow(final HttpClient httpClient, final Supplier<String> stateSupplier) {
    this(httpClient, stateSupplier, TokenRequestContentType.URL_ENCODED);
  }

  public BaseOAuth2Flow(final HttpClient httpClient,
                        final Supplier<String> stateSupplier,
                        final TokenRequestContentType tokenReqContentType) {
    super();
    this.httpClient = httpClient;
    this.stateSupplier = stateSupplier;
    this.tokenReqContentType = tokenReqContentType;
  }

  /**
   * Retrieves the content type to be used for the token request.
   *
   * @param inputOAuthConfiguration the OAuth configuration as a JsonNode
   * @return the content type for the token request, which is URL_ENCODED by default
   */
  protected TokenRequestContentType getRequestContentType(final JsonNode inputOAuthConfiguration) {
    return TokenRequestContentType.URL_ENCODED;
  }

  /**
   * Generates the consent URL for OAuth2 authentication for a given source.
   *
   * @param workspaceId the UUID of the workspace
   * @param sourceDefinitionId the UUID of the source definition
   * @param redirectUrl the URL to redirect to after authentication
   * @param inputOAuthConfiguration the input OAuth configuration as a JsonNode
   * @param oauthConfigSpecification the OAuth configuration specification
   * @param sourceOAuthParamConfig the source OAuth parameter configuration as a JsonNode
   * @return the formatted consent URL as a String
   * @throws IOException if an I/O error occurs
   * @throws JsonValidationException if the input OAuth configuration is invalid
   * @throws ResourceNotFoundProblem if the source OAuth parameter configuration is null
   */
  @Override
  public String getSourceConsentUrl(final UUID workspaceId,
                                    final UUID sourceDefinitionId,
                                    final String redirectUrl,
                                    final JsonNode inputOAuthConfiguration,
                                    final OAuthConfigSpecification oauthConfigSpecification,
                                    final JsonNode sourceOAuthParamConfig)
      throws IOException, JsonValidationException {
    validateInputOAuthConfiguration(oauthConfigSpecification, inputOAuthConfiguration);
    // This should probably never happen because the caller of this function should throw this exception
    // when fetching the param, but this was the prior behavior so adding it here.
    if (sourceOAuthParamConfig == null) {
      throw new ResourceNotFoundProblem(
          "Undefined OAuth Parameter.",
          new ProblemResourceData().resourceType(ConfigSchema.SOURCE_OAUTH_PARAM.name()));
    }

    return formatConsentUrl(sourceDefinitionId,
        getClientIdUnsafe(sourceOAuthParamConfig),
        redirectUrl,
        Jsons.mergeNodes(inputOAuthConfiguration, getOAuthDeclarativeInputSpec(oauthConfigSpecification)));
  }

  /**
   * Generates the consent URL for OAuth2 authorization for a destination.
   *
   * @param workspaceId the ID of the workspace requesting the consent URL
   * @param destinationDefinitionId the ID of the destination definition
   * @param redirectUrl the URL to redirect to after authorization
   * @param inputOAuthConfiguration the OAuth configuration input provided by the user
   * @param oauthConfigSpecification the specification for the OAuth configuration
   * @param destinationOAuthParamConfig the OAuth parameters configuration for the destination
   * @return the formatted consent URL for OAuth2 authorization
   * @throws IOException if an I/O error occurs
   * @throws JsonValidationException if the input OAuth configuration is invalid
   * @throws ResourceNotFoundProblem if the destination OAuth parameter configuration is not found
   */
  @Override
  public String getDestinationConsentUrl(final UUID workspaceId,
                                         final UUID destinationDefinitionId,
                                         final String redirectUrl,
                                         final JsonNode inputOAuthConfiguration,
                                         final OAuthConfigSpecification oauthConfigSpecification,
                                         final JsonNode destinationOAuthParamConfig)
      throws IOException, JsonValidationException {
    validateInputOAuthConfiguration(oauthConfigSpecification, inputOAuthConfiguration);
    // This should probably never happen because the caller of this function should throw this exception
    // when fetching the param, but this was the prior behavior so adding it here.
    if (destinationOAuthParamConfig == null) {
      throw new ResourceNotFoundProblem(
          "Undefined OAuth Parameter.",
          new ProblemResourceData().resourceType(ConfigSchema.DESTINATION_OAUTH_PARAM.name()));
    }

    return formatConsentUrl(destinationDefinitionId,
        getClientIdUnsafe(destinationOAuthParamConfig),
        redirectUrl,
        Jsons.mergeNodes(inputOAuthConfiguration, getOAuthDeclarativeInputSpec(oauthConfigSpecification)));
  }

  /**
   * Depending on the OAuth flow implementation, the URL to grant user's consent may differ,
   * especially in the query parameters to be provided. This function should generate such consent URL
   * accordingly.
   *
   * @param definitionId The configured definition ID of this client
   * @param clientId The configured client ID
   * @param redirectUrl the redirect URL
   * @param inputOAuthConfiguration any configuration property from connector necessary for this OAuth
   *        Flow
   */
  protected abstract String formatConsentUrl(final UUID definitionId,
                                             final String clientId,
                                             final String redirectUrl,
                                             final JsonNode inputOAuthConfiguration)
      throws IOException;

  private static String generateRandomState() {
    return RandomStringUtils.randomAlphanumeric(7);
  }

  /**
   * Generate a string to use as state in the OAuth process.
   */
  protected String getState() {
    return stateSupplier.get();
  }

  // Overload method to provide the `inputOAuthConfiguration` spec config.
  protected String getState(final JsonNode inputOAuthConfiguration) {
    return getState();
  }

  @Override
  @Deprecated
  public Map<String, Object> completeSourceOAuth(final UUID workspaceId,
                                                 final UUID sourceDefinitionId,
                                                 final Map<String, Object> queryParams,
                                                 final String redirectUrl,
                                                 final JsonNode oauthParamConfig)
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
            redirectUrl,
            Jsons.emptyObject(),
            oauthParamConfig,
            null),
        getDefaultOAuthOutputPath());

  }

  /**
   * Completes the OAuth2 flow for a source by validating the input OAuth configuration, handling any
   * ignored OAuth errors, merging the input configuration with the declarative input specification,
   * and formatting the OAuth output.
   *
   * @param workspaceId the ID of the workspace
   * @param sourceDefinitionId the ID of the source definition
   * @param queryParams the query parameters from the OAuth callback
   * @param redirectUrl the redirect URL used in the OAuth flow
   * @param inputOAuthConfiguration the input OAuth configuration
   * @param oauthConfigSpecification the OAuth configuration specification
   * @param oauthParamConfig the OAuth parameter configuration
   * @return a map containing the formatted OAuth output
   * @throws IOException if an I/O error occurs during the OAuth flow
   * @throws JsonValidationException if the input OAuth configuration is invalid
   */
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

    final JsonNode oauthConfigurationMerged = Jsons.mergeNodes(inputOAuthConfiguration, getOAuthDeclarativeInputSpec(oauthConfigSpecification));
    return formatOAuthOutput(
        oauthParamConfig,
        completeOAuthFlow(
            getClientIdUnsafe(oauthParamConfig),
            getClientSecretUnsafe(oauthParamConfig),
            extractCodeParameter(queryParams),
            redirectUrl,
            oauthConfigurationMerged,
            oauthParamConfig,
            extractStateParameter(queryParams, oauthConfigurationMerged)),
        oauthConfigSpecification);

  }

  @Override
  @Deprecated
  public Map<String, Object> completeDestinationOAuth(final UUID workspaceId,
                                                      final UUID destinationDefinitionId,
                                                      final Map<String, Object> queryParams,
                                                      final String redirectUrl,
                                                      final JsonNode oauthParamConfig)
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
            redirectUrl,
            Jsons.emptyObject(),
            oauthParamConfig,
            null),
        getDefaultOAuthOutputPath());

  }

  /**
   * Completes the OAuth flow for a destination by validating the input configuration, handling any
   * OAuth errors, merging the input configuration with the declarative input specification, and
   * formatting the OAuth output.
   *
   * @param workspaceId the ID of the workspace
   * @param destinationDefinitionId the ID of the destination definition
   * @param queryParams the query parameters from the OAuth callback
   * @param redirectUrl the redirect URL used in the OAuth flow
   * @param inputOAuthConfiguration the input OAuth configuration
   * @param oauthConfigSpecification the OAuth configuration specification
   * @param oauthParamConfig the OAuth parameter configuration
   * @return a map containing the formatted OAuth output
   * @throws IOException if an I/O error occurs during the OAuth flow
   * @throws JsonValidationException if the input OAuth configuration is invalid
   */
  @Override
  public Map<String, Object> completeDestinationOAuth(final UUID workspaceId,
                                                      final UUID destinationDefinitionId,
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

    final JsonNode oauthConfigurationMerged = Jsons.mergeNodes(inputOAuthConfiguration, getOAuthDeclarativeInputSpec(oauthConfigSpecification));
    return formatOAuthOutput(
        oauthParamConfig,
        completeOAuthFlow(
            getClientIdUnsafe(oauthParamConfig),
            getClientSecretUnsafe(oauthParamConfig),
            extractCodeParameter(queryParams),
            redirectUrl,
            oauthConfigurationMerged,
            oauthParamConfig,
            extractStateParameter(queryParams, oauthConfigurationMerged)),
        oauthConfigSpecification);

  }

  /**
   * Generates the headers required for completing the OAuth flow request.
   *
   * @param inputOAuthConfiguration the JSON node containing the OAuth configuration.
   * @return a map containing the headers for the OAuth flow request.
   * @throws IOException if an I/O error occurs.
   */
  protected Map<String, String> getCompleteOAuthFlowRequestHeaders(final String clientId,
                                                                   final String clientSecret,
                                                                   final String authCode,
                                                                   final String redirectUrl,
                                                                   final JsonNode inputOAuthConfiguration)
      throws IOException {

    final Map<String, String> requestHeaders = new HashMap<>();
    requestHeaders.put("Content-Type", tokenReqContentType.getContentType());
    requestHeaders.put("Accept", "application/json");

    return requestHeaders;
  }

  /**
   * Constructs an HTTP request to complete the OAuth2 flow.
   *
   * @param accessTokenUrl The URL to request the access token from.
   * @param accessTokenQueryParameters The query parameters to include in the access token request.
   * @param requestHeaders The headers to include in the HTTP request.
   * @param requestContentType The content type of the token request.
   * @param inputOAuthConfiguration The OAuth configuration input as a JsonNode.
   * @return The constructed HTTP request.
   * @throws IOException If an I/O error occurs when building the request.
   */
  protected HttpRequest getCompleteOAuthFlowHttpRequest(final String accessTokenUrl,
                                                        final Map<String, String> accessTokenQueryParameters,
                                                        final Map<String, String> requestHeaders,
                                                        final TokenRequestContentType requestContentType,
                                                        final JsonNode inputOAuthConfiguration)
      throws IOException {
    // prepare query params
    final Function<Map<String, String>, String> contentTypeConverter = requestContentType.getConverter();
    final String contentTypeConvertedParams = contentTypeConverter.apply(accessTokenQueryParameters);

    // prepare the request
    final Builder request = HttpRequest.newBuilder();
    request.POST(HttpRequest.BodyPublishers.ofString(contentTypeConvertedParams));
    request.uri(URI.create(accessTokenUrl));
    requestHeaders.forEach(request::header);

    // build the request
    return request.build();
  }

  /**
   * Complete OAuth flow overload to ensure backward compatibility, and provide the `state` param
   * input.
   *
   * @param clientId client id
   * @param clientSecret client secret
   * @param authCode oauth code
   * @param redirectUrl redirect url
   * @param inputOAuthConfiguration oauth configuration
   * @param oauthParamConfig oauth params
   * @param state state value
   * @return object returned from oauth flow
   * @throws IOException thrown while executing io
   */
  protected Map<String, Object> completeOAuthFlow(final String clientId,
                                                  final String clientSecret,
                                                  final String authCode,
                                                  final String redirectUrl,
                                                  final JsonNode inputOAuthConfiguration,
                                                  final JsonNode oauthParamConfig,
                                                  final String state)
      throws IOException {
    return getCompleteOAuthFlowOutput(
        formatAccessTokenUrl(
            getAccessTokenUrl(inputOAuthConfiguration), clientId, clientSecret, authCode, redirectUrl, inputOAuthConfiguration, state),
        getAccessTokenQueryParameters(clientId, clientSecret, authCode, redirectUrl, state, inputOAuthConfiguration),
        getCompleteOAuthFlowRequestHeaders(clientId, clientSecret, authCode, redirectUrl, inputOAuthConfiguration),
        getRequestContentType(inputOAuthConfiguration),
        inputOAuthConfiguration);

  }

  protected Map<String, Object> getCompleteOAuthFlowOutput(final String accessTokenUrl,
                                                           final Map<String, String> accessTokenQueryParameters,
                                                           final Map<String, String> requestHeaders,
                                                           final TokenRequestContentType requestContentType,
                                                           final JsonNode inputOAuthConfiguration)
      throws IOException {

    final HttpRequest request = getCompleteOAuthFlowHttpRequest(accessTokenUrl,
        accessTokenQueryParameters,
        requestHeaders,
        requestContentType,
        inputOAuthConfiguration);

    try {
      final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      return extractOAuthOutput(Jsons.deserialize(response.body()), accessTokenUrl, inputOAuthConfiguration);
    } catch (final InterruptedException e) {
      throw new IOException("Failed to complete OAuth flow", e);
    }
  }

  /**
   * Query parameters to provide the access token url with.
   */
  protected Map<String, String> getAccessTokenQueryParameters(final String clientId,
                                                              final String clientSecret,
                                                              final String authCode,
                                                              final String redirectUrl,
                                                              final String state,
                                                              final JsonNode inputOAuthConfiguration) {
    return getAccessTokenQueryParameters(clientId, clientSecret, authCode, redirectUrl);
  }

  /**
   * Query parameters to provide the access token url with.
   */
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
        .build();
  }

  /**
   * Once the user is redirected after getting their consent, the API should redirect them to a
   * specific redirection URL along with query parameters. This function should parse and extract the
   * code from these query parameters in order to continue the OAuth Flow.
   */
  protected String extractCodeParameter(final Map<String, Object> queryParams) throws IOException {
    if (queryParams.containsKey("code")) {
      return (String) queryParams.get("code");
    } else {
      throw new IOException("Undefined 'code' from consent redirected url.");
    }
  }

  /**
   * If there is an error param, return it.
   */
  protected String extractErrorParameter(final Map<String, Object> queryParams) {
    if (queryParams.containsKey("error")) {
      return (String) queryParams.get("error");
    } else {
      return null;
    }
  }

  protected boolean containsIgnoredOAuthError(final Map<String, Object> queryParams) {
    final String oauthError = extractErrorParameter(queryParams);
    return oauthError != null && ignoredOauthErrors.contains(oauthError);
  }

  /**
   * Return an error payload if the previous oAuth request was stopped.
   */
  protected Map<String, Object> buildRequestError(final Map<String, Object> queryParams) {
    final Map<String, Object> results = new HashMap<>();
    results.put("request_succeeded", false);
    results.put("request_error", extractErrorParameter(queryParams));
    return results;
  }

  /**
   * An Overload to return the accessTokenUrl with additional customisation.
   */
  protected abstract String getAccessTokenUrl(final JsonNode inputOAuthConfiguration);

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
  protected String formatAccessTokenUrl(final String accessTokenUrl,
                                        final String clientId,
                                        final String clientSecret,
                                        final String authCode,
                                        final String redirectUrl,
                                        final JsonNode inputOAuthConfiguration)
      throws IOException {

    return accessTokenUrl;
  }

  /**
   * Formats the access token URL with the provided parameters. Overload to ensure backward
   * compatibility, and provide the `state` param input.
   *
   * @param accessTokenUrl The base URL for obtaining the access token.
   * @param clientId The client ID for the OAuth application.
   * @param clientSecret The client secret for the OAuth application.
   * @param authCode The authorization code received from the OAuth provider.
   * @param redirectUrl The URL to which the OAuth provider will redirect after authorization.
   * @param inputOAuthConfiguration Additional OAuth configuration parameters.
   * @param state The state value
   * @return The formatted access token URL.
   */
  protected String formatAccessTokenUrl(final String accessTokenUrl,
                                        final String clientId,
                                        final String clientSecret,
                                        final String authCode,
                                        final String redirectUrl,
                                        final JsonNode inputOAuthConfiguration,
                                        final String state)
      throws IOException {

    return formatAccessTokenUrl(accessTokenUrl, clientId, clientSecret, authCode, redirectUrl, inputOAuthConfiguration);
  }

  /**
   * Extract all OAuth outputs from distant API response and store them in a flat map.
   */
  protected Map<String, Object> extractOAuthOutput(final JsonNode data, final String accessTokenUrl, final JsonNode inputOAuthConfiguration)
      throws IOException {
    return extractOAuthOutput(data, accessTokenUrl);
  }

  /**
   * Extract all OAuth outputs from distant API response and store them in a flat map.
   */
  protected Map<String, Object> extractOAuthOutput(final JsonNode data, final String accessTokenUrl) throws IOException {
    final Map<String, Object> result = new HashMap<>();
    if (data.has("refresh_token")) {
      result.put("refresh_token", data.get("refresh_token").asText());
    } else {
      throw new IOException(String.format("Missing 'refresh_token' in query params from %s", accessTokenUrl));
    }
    return result;
  }

  /**
   * This function should parse and extract the state from these query parameters in order to continue
   * the OAuth Flow.
   */
  protected String extractStateParameter(final Map<String, Object> queryParams, final JsonNode inputOAuthConfiguration) throws IOException {
    if (queryParams.containsKey("state")) {
      return (String) queryParams.get("state");
    } else {
      return null;
    }
  }

  @Override
  @Deprecated
  public List<String> getDefaultOAuthOutputPath() {
    return List.of("credentials");
  }

  protected static void validateInputOAuthConfiguration(final OAuthConfigSpecification oauthConfigSpecification,
                                                        final JsonNode inputOAuthConfiguration)
      throws JsonValidationException {
    if (oauthConfigSpecification != null && oauthConfigSpecification.getOauthUserInputFromConnectorConfigSpecification() != null) {
      final JsonSchemaValidator validator = new JsonSchemaValidator();
      validator.ensure(oauthConfigSpecification.getOauthUserInputFromConnectorConfigSpecification(), inputOAuthConfiguration);
    }
  }

  private static String urlEncode(final String s) {
    try {
      return URLEncoder.encode(s, StandardCharsets.UTF_8);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static String toUrlEncodedString(final Map<String, String> body) {
    final StringBuilder result = new StringBuilder();
    for (final var entry : body.entrySet()) {
      if (result.length() > 0) {
        result.append("&");
      }
      result.append(entry.getKey()).append("=").append(urlEncode(entry.getValue()));
    }
    return result.toString();
  }

  protected static String toJson(final Map<String, String> body) {
    final Gson gson = new Gson();
    final Type gsonType = new TypeToken<Map<String, String>>() {}.getType();
    return gson.toJson(body, gsonType);
  }

}
