/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.declarative;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.text.StringSubstitutor;
import org.apache.commons.text.lookup.StringLookup;
import org.apache.commons.text.lookup.StringLookupFactory;

/**
 * The {@code DeclarativeOAuthSpecHandler} class is responsible for interpolating OAuth
 * specifications with provided values. It maintains a map of interpolation values and provides
 * methods to reset the state, add values, and renderStringTemplate strings based on the input OAuth
 * configuration.
 */
public class DeclarativeOAuthSpecHandler {

  /**
   * The Airbyte Protocol declared literals for an easy access and reuse.
   */
  protected static final String ACCESS_TOKEN = "access_token";
  protected static final String ACCESS_TOKEN_HEADERS_KEY = "access_token_headers";
  protected static final String ACCESS_TOKEN_KEY = "access_token_key";
  protected static final String ACCESS_TOKEN_URL = "access_token_url";
  protected static final String AUTH_CODE_KEY = "auth_code_key";
  protected static final String AUTH_CODE_VALUE = "code";
  protected static final String CLIENT_ID_KEY = "client_id_key";
  protected static final String CLIENT_ID_VALUE = "client_id";
  protected static final String CLIENT_SECRET_KEY = "client_secret_key";
  protected static final String CLIENT_SECRET_VALUE = "client_secret";
  protected static final String CONSENT_URL = "consent_url";
  protected static final String EXTRACT_OUTPUT_KEY = "extract_output";
  protected static final String REDIRECT_URI_KEY = "redirect_uri_key";
  protected static final String REDIRECT_URI_VALUE = "redirect_uri";
  protected static final String REFRESH_TOKEN = "refresh_token";
  protected static final String SCOPE_KEY = "scope_key";
  protected static final String SCOPE_VALUE = "scope";
  protected static final String STATE_KEY = "state_key";
  protected static final String STATE_VALUE = "state";
  protected static final String STATE_PARAM_KEY = STATE_VALUE;

  /**
   * TypeReference to cover expected objects in the userConfig.
   */
  private static final TypeReference<Map<String, Integer>> STATE_TYPE_REF = new TypeReference<>() {};
  private static final TypeReference<Map<String, String>> COMPLETE_OAUTH_HEADERS_TYPE_REF = new TypeReference<>() {};
  private static final TypeReference<List<String>> EXTRACT_OUTPUT_TYPE_REF = new TypeReference<>() {};

  /**
   * The minimum and maximum length for the OAuth state parameter. This values is used to ensure that
   * the state parameter has a sufficient length, acceptable by data-provider.
   */
  private static final Integer STATE_LEN_MIN = 7;
  private static final Integer STATE_LEN_MAX = 24;

  /**
   * A string containing the characters allowed in the state parameter for OAuth. This includes
   * lowercase and uppercase letters, digits, and the characters '.', '-', and '_'.
   */
  private static final String STATE_ALLOWED_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_";

  /**
   * A map containing the mandatory default keys used in OAuth specifications. These keys are
   * essential for the OAuth process and are mapped to their respective default values.
   *
   */
  private static final Map<String, String> MANDATORY_DEFAULT_KEYS = Map.of(
      AUTH_CODE_KEY, AUTH_CODE_VALUE,
      CLIENT_ID_KEY, CLIENT_ID_VALUE,
      CLIENT_SECRET_KEY, CLIENT_SECRET_VALUE,
      REDIRECT_URI_KEY, REDIRECT_URI_VALUE,
      SCOPE_KEY, SCOPE_VALUE,
      STATE_KEY, STATE_VALUE);

  /**
   * A list of restricted context prefixes that are not allowed in the OAuth specification. These
   * prefixes are used to identify and restrict certain types of context that may pose security risks
   * or are not supported by the system.
   */
  private static final List<String> RESTRICTED_CONTEXT = List.of(
      "const:",
      "env:",
      "file:",
      "java:",
      "localhost:",
      "properties:",
      "resourceBundle:",
      "sys:");

  /**
   * A secure random number generator instance used for generating cryptographically strong random
   * values.
   */
  private static final SecureRandom secureRandom = new SecureRandom();

  /**
   * Creates and configures a StringSubstitutor for interpolating variables within strings.
   *
   * @param templateValues a map containing the template values to be used for interpolation.
   * @return a configured StringSubstitutor instance.
   */
  private static StringSubstitutor getInterpolator(final Map<String, String> templateValues) {

    final StringLookup defaultResolver = StringLookupFactory.INSTANCE.interpolatorStringLookup(templateValues);
    final StringLookup resolver = new CodeChallengeLookup(defaultResolver);
    final StringSubstitutor interpolator = new StringSubstitutor(resolver);

    interpolator.setVariablePrefix("{");
    interpolator.setEnableSubstitutionInVariables(true);
    interpolator.setEnableUndefinedVariableException(true);

    return interpolator;

  }

  /**
   * Retrieves the state key from the provided user configuration.
   *
   * @param userConfig the JSON node containing the user configuration
   * @return the state key as a string, or a default value if the state key is not present
   */
  protected final String getStateKey(final JsonNode userConfig) {
    return userConfig.path(STATE_KEY).asText(STATE_VALUE);
  }

  protected final String getConfigurableState(final JsonNode stateConfig) {

    final Map<String, Integer> state = Jsons.object(stateConfig, STATE_TYPE_REF);
    final int min = state.getOrDefault("min", STATE_LEN_MIN);
    final int max = state.getOrDefault("max", STATE_LEN_MAX);
    final int length = secureRandom.nextInt((max - min) + 1) + min;

    final StringBuilder stateValue = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      stateValue.append(STATE_ALLOWED_CHARS.charAt(secureRandom.nextInt(STATE_ALLOWED_CHARS.length())));
    }

    return stateValue.toString();

  }

  /**
   * Parses the input OAuth configuration and adds all keys from the configuration to the
   * interpolation values map.
   *
   * @param userConfig the JSON node containing the input OAuth configuration.
   * @return the Map of (String, String) as parsedConfig.
   */
  protected final Map<String, String> createDefaultTemplateMap(final JsonNode userConfig) {

    final Map<String, String> templateMap = new HashMap<>();

    // populate interpolation values with mandatory keys, using override checks from user's input
    MANDATORY_DEFAULT_KEYS.forEach((key, defaultValue) -> {
      templateMap.put(key, userConfig.path(key).asText(defaultValue));
    });

    // process all other key:value from `userConfig`
    userConfig.fields().forEachRemaining(item -> {
      templateMap.put(item.getKey(), item.getValue().asText());
    });

    return templateMap;
  }

  /**
   * Generates a map of template values for constructing a consent URL.
   *
   * @param userConfig The JSON node containing the OAuth configuration.
   * @param clientId The client ID to be included in the template values.
   * @param redirectUrl The redirect URL to be included in the template values.
   * @param state The state value to be included in the template values.
   * @return A map containing the template values for the consent URL.
   */
  protected Map<String, String> getConsentUrlTemplateValues(final JsonNode userConfig,
                                                            final String clientId,
                                                            final String redirectUrl,
                                                            final String state) {

    final Map<String, String> templateValues = createDefaultTemplateMap(userConfig);
    templateValues.put(templateValues.get(CLIENT_ID_KEY), clientId);
    templateValues.put(templateValues.get(REDIRECT_URI_KEY), redirectUrl);
    templateValues.put(templateValues.get(STATE_KEY), state);

    return templateValues;

  }

  /**
   * Populates a map with OAuth header template values.
   *
   * @param userConfig the JSON node containing the input OAuth configuration
   * @param clientId the client ID to be included in the headers
   * @param clientSecret the client secret to be included in the headers
   * @param authCode the authorization code to be included in the headers
   * @param redirectUrl the redirect URL to be included in the headers
   * @return a map containing the complete OAuth header template values
   */
  protected Map<String, String> getCompleteOAuthHeadersTemplateValues(final JsonNode userConfig,
                                                                      final String clientId,
                                                                      final String clientSecret,
                                                                      final String authCode,
                                                                      final String redirectUrl) {

    final Map<String, String> templateValues = createDefaultTemplateMap(userConfig);
    templateValues.put(templateValues.get(CLIENT_ID_KEY), clientId);
    templateValues.put(templateValues.get(CLIENT_SECRET_KEY), clientSecret);
    templateValues.put(templateValues.get(AUTH_CODE_KEY), authCode);
    templateValues.put(templateValues.get(REDIRECT_URI_KEY), redirectUrl);

    return templateValues;

  }

  /**
   * Generates a map of template values required for constructing the access token URL.
   *
   * @param userConfig The JSON node containing the OAuth configuration.
   * @param clientId The client ID for the OAuth application.
   * @param clientSecret The client secret for the OAuth application.
   * @param authCode The authorization code received from the OAuth provider.
   * @param redirectUrl The redirect URL used in the OAuth flow.
   * @param state The state parameter used in the OAuth flow, can be null.
   * @return A map containing the template values for the access token URL.
   */
  protected Map<String, String> getAccessTokenUrlTemplateValues(final JsonNode userConfig,
                                                                final String clientId,
                                                                final String clientSecret,
                                                                final String authCode,
                                                                final String redirectUrl,
                                                                final String state) {

    final Map<String, String> templateValues = createDefaultTemplateMap(userConfig);
    templateValues.put(templateValues.get(CLIENT_ID_KEY), clientId);
    templateValues.put(templateValues.get(CLIENT_SECRET_KEY), clientSecret);
    templateValues.put(templateValues.get(AUTH_CODE_KEY), authCode);
    templateValues.put(templateValues.get(REDIRECT_URI_KEY), redirectUrl);
    templateValues.put(templateValues.get(STATE_KEY), state);

    return templateValues;

  }

  /**
   * Renders a string template by replacing placeholders with corresponding values from the provided
   * map.
   *
   * @param templateValues a map containing the placeholder values to be used in the template.
   * @param templateString the string template containing placeholders to be replaced.
   * @return the rendered string with placeholders replaced by their corresponding values.
   * @throws IOException if an I/O error occurs during the rendering process.
   */
  protected final String renderStringTemplate(final Map<String, String> templateValues, final String templateString) throws IOException {

    try {
      return getInterpolator(templateValues).replace(checkContext(templateString));
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }

  }

  /**
   * Checks if the provided templateString contains any restricted interpolation context items. If any
   * restricted item is found, an IOException is thrown.
   *
   * @param templateString the string to be checked for restricted interpolation context items.
   * @return the original templateString if no restricted items are found.
   * @throws IOException if the templateString contains any restricted interpolation context items.
   */
  private String checkContext(final String templateString) throws IOException {

    for (final String item : RESTRICTED_CONTEXT) {
      if (templateString.contains(item)) {
        final String errorMsg = "DeclarativeOAuthSpecHandler(): the `%s` usage in `%s` is not allowed for string interpolation.";
        throw new IOException(String.format(errorMsg, item, templateString));
      }
    }

    return templateString;
  }

  /**
   * Retrieves the configuration extract output from the provided JSON node.
   *
   * This method checks if the input JSON node contains the "extract_output" field. If the field is
   * missing, it throws an IOException indicating that the field is mandatory. If the field is
   * present, it retrieves the value as a list of strings.
   *
   * If the retrieved list is not empty, it returns the list. Otherwise, it returns a default list
   * containing "refresh_token".
   *
   * @param userConfig the JSON node containing the OAuth configuration.
   * @return a list of strings representing the configuration extract output.
   */
  protected final List<String> getConfigExtractOutput(final JsonNode userConfig) {

    final JsonNode value = userConfig.path(EXTRACT_OUTPUT_KEY);
    final List<String> extractOutputConfig = value.isMissingNode()
        ? List.of()
        : Jsons.object(value, EXTRACT_OUTPUT_TYPE_REF);

    // match the default BaseOAuth2Flow behaviour
    return (!extractOutputConfig.isEmpty()) ? extractOutputConfig : List.of(REFRESH_TOKEN);
  }

  /**
   * Retrieves and interpolates access token headers from the provided configuration.
   *
   * @param userConfig the JSON configuration node containing access token headers.
   * @return a map of interpolated access token headers.
   * @throws IOException if an I/O error occurs during interpolation
   */
  protected final Map<String, String> renderCompleteOAuthHeaders(final Map<String, String> templateValues,
                                                                 final JsonNode userConfig)
      throws IOException {

    final JsonNode value = userConfig.path(ACCESS_TOKEN_HEADERS_KEY);
    final Map<String, String> userHeaders = value.isMissingNode()
        ? new HashMap<>()
        : Jsons.object(value, COMPLETE_OAUTH_HEADERS_TYPE_REF);

    final Map<String, String> accessTokenHeadersRendered = new HashMap<>();
    userHeaders.forEach((headerKey, headerValue) -> {
      try {
        accessTokenHeadersRendered.put(
            renderStringTemplate(templateValues, headerKey),
            renderStringTemplate(templateValues, headerValue));
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    });

    return accessTokenHeadersRendered;
  }

}