/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.declarative;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.json.JsonPaths;
import io.airbyte.commons.json.Jsons;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
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

  private Clock clock;
  private final SecureRandom secureRandom;

  /**
   * Constructor for DeclarativeOAuthSpecHandler. Initializes the clock to the system's UTC clock and
   * the secureRandom to a new instance of SecureRandom.
   */
  protected DeclarativeOAuthSpecHandler() {
    this.clock = Clock.systemUTC();
    this.secureRandom = new SecureRandom();
  }

  /**
   * Sets the clock instance to be used by this handler, mainly for the testing purposes.
   *
   * @param clock the Clock instance to set
   */
  @VisibleForTesting
  public void setClock(final Clock clock) {
    this.clock = clock;
  }

  /**
   * The Airbyte Protocol declared literals for an easy access and reuse.
   */
  protected static final String ACCESS_TOKEN = "access_token";
  protected static final String ACCESS_TOKEN_HEADERS_KEY = "access_token_headers";
  protected static final String ACCESS_TOKEN_KEY = "access_token_key";
  protected static final String ACCESS_TOKEN_PARAMS_KEY = "access_token_params";
  protected static final String ACCESS_TOKEN_URL = "access_token_url";
  protected static final String AUTH_CODE_KEY = "auth_code_key";
  protected static final String AUTH_CODE_PARAM = "auth_code_param";
  protected static final String AUTH_CODE_VALUE = "code";
  protected static final String AUTH_CODE_VALUE_KEY = "auth_code_value";
  protected static final String CLIENT_ID_KEY = "client_id_key";
  protected static final String CLIENT_ID_PARAM = "client_id_param";
  protected static final String CLIENT_ID_VALUE = "client_id";
  protected static final String CLIENT_ID_VALUE_KEY = "client_id_value";
  protected static final String CLIENT_SECRET_KEY = "client_secret_key";
  protected static final String CLIENT_SECRET_PARAM = "client_secret_param";
  protected static final String CLIENT_SECRET_VALUE = "client_secret";
  protected static final String CLIENT_SECRET_VALUE_KEY = "client_secret_value";
  protected static final String CONSENT_URL = "consent_url";
  protected static final String EXTRACT_OUTPUT_KEY = "extract_output";
  protected static final String REDIRECT_URI_KEY = "redirect_uri_key";
  protected static final String REDIRECT_URI_PARAM = "redirect_uri_param";
  protected static final String REDIRECT_URI_VALUE = "redirect_uri";
  protected static final String REDIRECT_URI_VALUE_KEY = "redirect_uri_value";
  protected static final String REFRESH_TOKEN = "refresh_token";
  protected static final String SCOPE_KEY = "scope_key";
  protected static final String SCOPE_PARAM = "scope_param";
  protected static final String SCOPE_VALUE = "scope";
  protected static final String SCOPE_VALUE_KEY = "scope_value";
  protected static final String STATE_KEY = "state_key";
  protected static final String STATE_PARAM = "state_param";
  protected static final String STATE_VALUE = "state";
  protected static final String STATE_VALUE_KEY = "state_value";
  protected static final String STATE_PARAM_KEY = STATE_VALUE;
  protected static final String STATE_PARAM_MIN_KEY = "min";
  protected static final String STATE_PARAM_MAX_KEY = "max";
  protected static final String TOKEN_EXPIRY_KEY = "token_expiry_key";
  protected static final String TOKEN_EXPIRY_VALUE = "expires_in";
  protected static final String TOKEN_EXPIRY_DATE_KEY = "token_expiry_date";

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
      "const",
      "env",
      "file",
      "java",
      "localhost",
      "properties",
      "resourceBundle",
      "sys");

  /**
   * Creates and configures a StringSubstitutor for interpolating variables within strings.
   *
   * @param templateValues a map containing the template values to be used for interpolation.
   * @return a configured StringSubstitutor instance.
   */
  private static StringSubstitutor getInterpolator(final Map<String, String> templateValues) {

    final StringLookup baseResolver = StringLookupFactory.INSTANCE.interpolatorStringLookup(templateValues);
    final StringLookup customResolver = new CodeChallengeS256Lookup(baseResolver);
    final StringLookup resolver = new JinjaStringLookup(customResolver);
    final StringSubstitutor interpolator = new StringSubstitutor(resolver);

    interpolator.setVariablePrefix("{{");
    interpolator.setVariableSuffix("}}");
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

  /**
   * Retrieves the authorization code key from the provided user configuration.
   *
   * @param userConfig the JSON node containing user configuration details
   * @return the authorization code key as a string
   */
  protected final String getAuthCodeKey(final JsonNode userConfig) {
    return userConfig.path(AUTH_CODE_KEY).asText(AUTH_CODE_VALUE);
  }

  /**
   * Retrieves the client ID key from the provided user configuration.
   *
   * @param userConfig the JSON node containing the user configuration
   * @return the client ID key as a string, or a default value if the key is not present
   */
  protected final String getClientIdKey(final JsonNode userConfig) {
    return userConfig.path(CLIENT_ID_KEY).asText(CLIENT_ID_VALUE);
  }

  /**
   * Retrieves the client secret key from the provided user configuration.
   *
   * @param userConfig the JSON node containing the user configuration
   * @return the client secret key as a string, or a default value if the key is not present
   */
  protected final String getClientSecretKey(final JsonNode userConfig) {
    return userConfig.path(CLIENT_SECRET_KEY).asText(CLIENT_SECRET_VALUE);
  }

  protected final String getTokenExpiryKey(final JsonNode userConfig) {
    return userConfig.path(TOKEN_EXPIRY_KEY).asText(TOKEN_EXPIRY_VALUE);
  }

  /**
   * Generates a configurable state string based on the provided JSON configuration.
   *
   * @param stateConfig the JSON node containing the state configuration with optional "min" and "max"
   *        length values.
   * @return a randomly generated state string of a length between the specified "min" and "max"
   *         values, inclusive. If "min" or "max" are not provided in the configuration, default
   *         values are used.
   */
  protected final String getConfigurableState(final JsonNode stateConfig) {

    final Map<String, Integer> userState = Jsons.deserializeToIntegerMap(stateConfig);
    final int min = userState.getOrDefault(STATE_PARAM_MIN_KEY, STATE_LEN_MIN);
    final int max = userState.getOrDefault(STATE_PARAM_MAX_KEY, STATE_LEN_MAX);
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
   * Adds a reference to the template values map.
   *
   * @param templateValues the map containing template values
   * @param key the key to retrieve the value from the template values map
   * @param paramKey the key under which the reference will be stored in the template values map
   * @param encode a boolean indicating whether the value should be URL encoded
   */
  private void addParameterReference(final Map<String, String> templateValues,
                                     final String key,
                                     final String paramKey,
                                     final boolean encode) {
    final String value = templateValues.get(templateValues.get(key));
    templateValues.put(paramKey, makeParameter(templateValues.get(key), encode ? urlEncode(value) : value));
  }

  /**
   * Adds a value reference to the templateValues map.
   *
   * This method retrieves the value associated with the key in the templateValues map, and then puts
   * this value into the map with the specified valueKey.
   *
   * @param templateValues the map containing template values
   * @param key the key whose associated value is to be retrieved
   * @param valueKey the key with which the retrieved value is to be associated
   */
  private void addValueReference(final Map<String, String> templateValues,
                                 final String key,
                                 final String valueKey) {
    final String value = templateValues.get(templateValues.get(key));
    templateValues.put(valueKey, value);
  }

  /**
   * Populates the provided template values map with references for various OAuth parameters. Some
   * parameters are URL-encoded by default.
   *
   * @param templateValues a map containing the template values to be populated with references
   * @return the updated map with added references
   */
  protected Map<String, String> getTemplateParametersAndValues(final Map<String, String> templateValues) {
    addParameterReference(templateValues, CLIENT_ID_KEY, CLIENT_ID_PARAM, false);
    addParameterReference(templateValues, CLIENT_SECRET_KEY, CLIENT_SECRET_PARAM, false);
    addParameterReference(templateValues, AUTH_CODE_KEY, AUTH_CODE_PARAM, false);
    addParameterReference(templateValues, STATE_KEY, STATE_PARAM, false);
    // urlEncode the `redirect_uri` and `scope` by default
    addParameterReference(templateValues, REDIRECT_URI_KEY, REDIRECT_URI_PARAM, true);
    addParameterReference(templateValues, SCOPE_KEY, SCOPE_PARAM, true);

    // add more value references to increase the granularity and flexibility
    addValueReference(templateValues, CLIENT_ID_KEY, CLIENT_ID_VALUE_KEY);
    addValueReference(templateValues, CLIENT_SECRET_KEY, CLIENT_SECRET_VALUE_KEY);
    addValueReference(templateValues, AUTH_CODE_KEY, AUTH_CODE_VALUE_KEY);
    addValueReference(templateValues, STATE_KEY, STATE_VALUE_KEY);
    addValueReference(templateValues, REDIRECT_URI_KEY, REDIRECT_URI_VALUE_KEY);
    addValueReference(templateValues, SCOPE_KEY, SCOPE_VALUE_KEY);

    return templateValues;
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

    return getTemplateParametersAndValues(templateValues);
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

    return getTemplateParametersAndValues(templateValues);
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

    return getTemplateParametersAndValues(templateValues);
  }

  /**
   * Generates a map of template values required for obtaining an access token.
   *
   * @param userConfig the user configuration as a JsonNode
   * @param clientId the client ID for OAuth
   * @param clientSecret the client secret for OAuth
   * @param authCode the authorization code received from the authorization server
   * @param redirectUrl the redirect URI used in the OAuth flow
   * @param state the state parameter to maintain state between the request and callback
   * @return a map containing the template values for the access token request
   */
  protected Map<String, String> getAccessTokenParamsTemplateValues(final JsonNode userConfig,
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

    return getTemplateParametersAndValues(templateValues);
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
    final String cleanedTemplatedString = removeWhitespaces(templateString);

    try {
      checkContext(cleanedTemplatedString);
      return getInterpolator(templateValues).replace(cleanedTemplatedString);
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
  protected void checkContext(final String templateString) throws IOException {

    for (final String item : RESTRICTED_CONTEXT) {
      if (templateString.contains(item)) {
        final String errorMsg = "DeclarativeOAuthSpecHandler(): the `%s` usage in `%s` is not allowed for string interpolation.";
        throw new IOException(String.format(errorMsg, item, templateString));
      }
    }
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
    final List<String> extractOutputConfig = Jsons.deserializeToStringList(
        Jsons.getNodeOrEmptyObject(userConfig, EXTRACT_OUTPUT_KEY));

    // match the default BaseOAuth2Flow behaviour, returning ["refresh_token"] by default.
    return !extractOutputConfig.isEmpty() ? extractOutputConfig : List.of(REFRESH_TOKEN);
  }

  /**
   * Renders the access token parameters by replacing placeholders in the parameter keys and values
   * with the corresponding values from the provided template values.
   *
   * @param templateValues a map containing the template values to be used for rendering the access
   *        token parameters
   * @param userConfig a JsonNode containing the user configuration, which includes the access token
   *        parameters
   * @return a map with the rendered access token parameters
   * @throws RuntimeException if an IOException occurs during the rendering of the string templates
   */
  protected final Map<String, String> renderConfigAccessTokenParams(final Map<String, String> templateValues,
                                                                    final JsonNode userConfig) {

    final Map<String, String> accessTokenParamsRendered = new HashMap<>();
    final Map<String, String> userAccessTokenParams = Jsons.deserializeToStringMap(
        Jsons.getNodeOrEmptyObject(userConfig, ACCESS_TOKEN_PARAMS_KEY));

    userAccessTokenParams.forEach((paramKey, paramValue) -> {
      try {
        accessTokenParamsRendered.put(
            renderStringTemplate(templateValues, paramKey),
            renderStringTemplate(templateValues, paramValue));
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    });

    return accessTokenParamsRendered;
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

    final Map<String, String> accessTokenHeadersRendered = new HashMap<>();
    final Map<String, String> userHeaders = Jsons.deserializeToStringMap(
        Jsons.getNodeOrEmptyObject(userConfig, ACCESS_TOKEN_HEADERS_KEY));

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

  /**
   * Processes the OAuth output by extracting necessary fields from the provided data and user
   * configuration.
   *
   * @param userConfig The user configuration as a JsonNode.
   * @param data The data containing OAuth output as a JsonNode.
   * @param accessTokenUrl The URL used to obtain the access token.
   * @return A map containing the processed OAuth output.
   * @throws IOException If a required field is missing in the data.
   */
  protected Map<String, Object> processOAuthOutput(final JsonNode userConfig,
                                                   final JsonNode data,
                                                   final String accessTokenUrl)
      throws IOException {

    final Map<String, Object> oauth_output = new HashMap<>();
    final List<String> expectedOAuthOuputFields = getConfigExtractOutput(userConfig);
    final List<String> availableOAuthOuputFields = getAvailableKeysForOAuthOutput(data);

    for (final String path : expectedOAuthOuputFields) {
      final String value = JsonPaths.getSingleValueTextOrNull(data, path);
      final String key = JsonPaths.getTargetKeyFromJsonPath(path);

      if (value != null) {
        // handle `token_expiry_key`
        if (getTokenExpiryKey(userConfig).equals(key)) {
          oauth_output.put(TOKEN_EXPIRY_DATE_KEY, processExpiresIn(value));
        }

        oauth_output.put(key, value);
      } else {
        final String message = "Missing '%s' field in the `OAuth Output`. Expected fields: %s. Fields available: %s";
        throw new IOException(String.format(message, key, expectedOAuthOuputFields, availableOAuthOuputFields));
      }
    }

    return oauth_output;
  }

  /**
   * Retrieves a list of all available keys from the given JSON node.
   *
   * @param data the JSON node containing the data
   * @return a list of strings representing the keys available in the JSON node
   */
  private List<String> getAvailableKeysForOAuthOutput(final JsonNode data) {
    final List<String> keys = new ArrayList<>();
    data.fieldNames().forEachRemaining(keys::add);
    return keys;
  }

  /**
   * Processes the expiration time by adding the specified number of seconds to the current time.
   *
   * @param value the number of seconds to add to the current time, represented as a string
   * @return a string representation of the new expiration time
   */
  private String processExpiresIn(final String value) {
    return Instant.now(clock).plusSeconds(Integer.parseInt(value)).toString();
  }

  /**
   * Constructs a reference string by formatting the given key and value.
   *
   * @param key the key to be included in the reference string
   * @param value the value to be included in the reference string
   * @return a formatted string in the form "key=value"
   */
  private String makeParameter(final String key, final String value) {
    return String.format("%s=%s", key, value);
  }

  /**
   * Removes all whitespace characters from the given template string.
   *
   * @param templateString the string from which to remove whitespace characters
   * @return a new string with all whitespace characters removed
   * @throws IOException if an I/O error occurs
   */
  private String removeWhitespaces(final String templateString) throws IOException {
    return templateString.replaceAll("\\s", "");
  }

  /**
   * Encodes the given string using the UTF-8 encoding scheme.
   *
   * @param s the string to be encoded; if null, the method returns null
   * @return the encoded string, or null if the input string is null
   * @throws RuntimeException if the encoding process fails
   */
  private static String urlEncode(final String s) {
    if (s == null) {
      return s;
    }

    try {
      return URLEncoder.encode(s, StandardCharsets.UTF_8);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

}
