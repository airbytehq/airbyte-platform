/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.declarative

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
import com.hubspot.jinjava.Jinjava
import io.airbyte.commons.json.JsonPaths
import io.airbyte.commons.json.Jsons
import java.io.IOException
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import java.time.Clock
import java.time.Instant

/**
 * The `DeclarativeOAuthSpecHandler` class is responsible for interpolating OAuth
 * specifications with provided values. It maintains a map of interpolation values and provides
 * methods to reset the state, add values, and renderStringTemplate strings based on the input OAuth
 * configuration.
 */
class DeclarativeOAuthSpecHandler {
  private var clock: Clock
  private val secureRandom: SecureRandom

  /**
   * Sets the clock instance to be used by this handler, mainly for the testing purposes.
   *
   * @param clock the Clock instance to set
   */
  @VisibleForTesting
  fun setClock(clock: Clock) {
    this.clock = clock
  }

  /**
   * Constructor for DeclarativeOAuthSpecHandler. Initializes the clock to the system's UTC clock and
   * the secureRandom to a new instance of SecureRandom.
   */
  init {
    this.clock = Clock.systemUTC()
    this.secureRandom = SecureRandom()
  }

  /**
   * Retrieves the state key from the provided user configuration.
   *
   * @param userConfig the JSON node containing the user configuration
   * @return the state key as a string, or a default value if the state key is not present
   */
  fun getStateKey(userConfig: JsonNode): String = userConfig.path(STATE_KEY).asText(STATE_VALUE)

  /**
   * Retrieves the authorization code key from the provided user configuration.
   *
   * @param userConfig the JSON node containing user configuration details
   * @return the authorization code key as a string
   */
  internal fun getAuthCodeKey(userConfig: JsonNode): String = userConfig.path(AUTH_CODE_KEY).asText(AUTH_CODE_VALUE)

  /**
   * Retrieves the client ID key from the provided user configuration.
   *
   * @param userConfig the JSON node containing the user configuration
   * @return the client ID key as a string, or a default value if the key is not present
   */
  internal fun getClientIdKey(userConfig: JsonNode): String = userConfig.path(CLIENT_ID_KEY).asText(CLIENT_ID_VALUE)

  /**
   * Retrieves the client secret key from the provided user configuration.
   *
   * @param userConfig the JSON node containing the user configuration
   * @return the client secret key as a string, or a default value if the key is not present
   */
  internal fun getClientSecretKey(userConfig: JsonNode): String = userConfig.path(CLIENT_SECRET_KEY).asText(CLIENT_SECRET_VALUE)

  protected fun getTokenExpiryKey(userConfig: JsonNode): String = userConfig.path(TOKEN_EXPIRY_KEY).asText(TOKEN_EXPIRY_VALUE)

  /**
   * Generates a configurable state string based on the provided JSON configuration.
   *
   * @param stateConfig the JSON node containing the state configuration with optional "min" and "max"
   * length values.
   * @return a randomly generated state string of a length between the specified "min" and "max"
   * values, inclusive. If "min" or "max" are not provided in the configuration, default
   * values are used.
   */
  fun getConfigurableState(stateConfig: JsonNode): String {
    val userState = Jsons.deserializeToIntegerMap(stateConfig)
    val min = userState[STATE_PARAM_MIN_KEY] ?: STATE_LEN_MIN
    val max = userState[STATE_PARAM_MAX_KEY] ?: STATE_LEN_MAX
    val length = secureRandom.nextInt((max - min) + 1) + min

    val stateValue = StringBuilder(length)
    for (i in 0..<length) {
      stateValue.append(STATE_ALLOWED_CHARS[secureRandom.nextInt(STATE_ALLOWED_CHARS.length)])
    }

    return stateValue.toString()
  }

  /**
   * Parses the input OAuth configuration and adds all keys from the configuration to the
   * interpolation values map.
   *
   * @param userConfig the JSON node containing the input OAuth configuration.
   * @return the Map of (String, String) as parsedConfig.
   */
  fun createDefaultTemplateMap(userConfig: JsonNode): MutableMap<String?, String?> {
    val templateMap: MutableMap<String?, String?> = HashMap()

    // populate interpolation values with mandatory keys, using override checks from user's input
    MANDATORY_DEFAULT_KEYS.forEach { (key: String?, defaultValue: String?) ->
      templateMap[key] = userConfig.path(key).asText(defaultValue)
    }

    // process all other key:value from `userConfig`
    userConfig.fields().forEachRemaining { item: Map.Entry<String?, JsonNode> ->
      templateMap[item.key] = item.value.asText()
    }

    return templateMap
  }

  /**
   * Adds a reference to the template values map.
   *
   * @param templateValues the map containing template values
   * @param key the key to retrieve the value from the template values map
   * @param paramKey the key under which the reference will be stored in the template values map
   * @param encode a boolean indicating whether the value should be URL encoded
   */
  private fun addParameterReference(
    templateValues: MutableMap<String?, String?>,
    key: String,
    paramKey: String,
    encode: Boolean,
  ) {
    val value = templateValues[templateValues[key]]
    templateValues[paramKey] =
      makeParameter(templateValues[key], if (encode) urlEncode(value) else value)
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
  private fun addValueReference(
    templateValues: MutableMap<String?, String?>,
    key: String,
    valueKey: String,
  ) {
    val value = templateValues[templateValues[key]]
    templateValues[valueKey] = value
  }

  /**
   * Populates the provided template values map with references for various OAuth parameters. Some
   * parameters are URL-encoded by default.
   *
   * @param templateValues a map containing the template values to be populated with references
   * @return the updated map with added references
   */
  protected fun getTemplateParametersAndValues(templateValues: MutableMap<String?, String?>): Map<String?, String?> {
    addParameterReference(templateValues, CLIENT_ID_KEY, CLIENT_ID_PARAM, false)
    addParameterReference(templateValues, CLIENT_SECRET_KEY, CLIENT_SECRET_PARAM, false)
    addParameterReference(templateValues, AUTH_CODE_KEY, AUTH_CODE_PARAM, false)
    addParameterReference(templateValues, STATE_KEY, STATE_PARAM, false)
    // urlEncode the `redirect_uri` and `scope` by default
    addParameterReference(templateValues, REDIRECT_URI_KEY, REDIRECT_URI_PARAM, true)
    addParameterReference(templateValues, SCOPE_KEY, SCOPE_PARAM, true)

    // add more value references to increase the granularity and flexibility
    addValueReference(templateValues, CLIENT_ID_KEY, CLIENT_ID_VALUE_KEY)
    addValueReference(templateValues, CLIENT_SECRET_KEY, CLIENT_SECRET_VALUE_KEY)
    addValueReference(templateValues, AUTH_CODE_KEY, AUTH_CODE_VALUE_KEY)
    addValueReference(templateValues, STATE_KEY, STATE_VALUE_KEY)
    addValueReference(templateValues, REDIRECT_URI_KEY, REDIRECT_URI_VALUE_KEY)
    addValueReference(templateValues, SCOPE_KEY, SCOPE_VALUE_KEY)

    return templateValues
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
  fun getConsentUrlTemplateValues(
    userConfig: JsonNode,
    clientId: String?,
    redirectUrl: String?,
    state: String?,
  ): Map<String?, String?> {
    val templateValues = createDefaultTemplateMap(userConfig)
    templateValues[templateValues[CLIENT_ID_KEY]] = clientId
    templateValues[templateValues[REDIRECT_URI_KEY]] = redirectUrl
    templateValues[templateValues[STATE_KEY]] = state

    return getTemplateParametersAndValues(templateValues)
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
  internal fun getCompleteOAuthHeadersTemplateValues(
    userConfig: JsonNode,
    clientId: String?,
    clientSecret: String?,
    authCode: String?,
    redirectUrl: String?,
  ): Map<String?, String?> {
    val templateValues = createDefaultTemplateMap(userConfig)
    templateValues[templateValues[CLIENT_ID_KEY]] = clientId
    templateValues[templateValues[CLIENT_SECRET_KEY]] = clientSecret
    templateValues[templateValues[AUTH_CODE_KEY]] = authCode
    templateValues[templateValues[REDIRECT_URI_KEY]] = redirectUrl

    return getTemplateParametersAndValues(templateValues)
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
  internal fun getAccessTokenUrlTemplateValues(
    userConfig: JsonNode,
    clientId: String?,
    clientSecret: String?,
    authCode: String?,
    redirectUrl: String?,
    state: String?,
  ): Map<String?, String?> {
    val templateValues = createDefaultTemplateMap(userConfig)
    templateValues[templateValues[CLIENT_ID_KEY]] = clientId
    templateValues[templateValues[CLIENT_SECRET_KEY]] = clientSecret
    templateValues[templateValues[AUTH_CODE_KEY]] = authCode
    templateValues[templateValues[REDIRECT_URI_KEY]] = redirectUrl
    templateValues[templateValues[STATE_KEY]] = state

    return getTemplateParametersAndValues(templateValues)
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
  internal fun getAccessTokenParamsTemplateValues(
    userConfig: JsonNode,
    clientId: String?,
    clientSecret: String?,
    authCode: String?,
    redirectUrl: String?,
    state: String?,
  ): Map<String?, String?> {
    val templateValues = createDefaultTemplateMap(userConfig)
    templateValues[templateValues[CLIENT_ID_KEY]] = clientId
    templateValues[templateValues[CLIENT_SECRET_KEY]] = clientSecret
    templateValues[templateValues[AUTH_CODE_KEY]] = authCode
    templateValues[templateValues[REDIRECT_URI_KEY]] = redirectUrl
    templateValues[templateValues[STATE_KEY]] = state

    return getTemplateParametersAndValues(templateValues)
  }

  /**
   * Renders a string template by interpolating the provided template values.
   *
   * @param templateValues a map containing the template variables and their corresponding values
   * @param templateString the string template to be rendered
   * @return the rendered string with the template variables replaced by their corresponding values
   * @throws IOException if an I/O error occurs during rendering
   */
  @Throws(IOException::class)
  fun renderStringTemplate(
    templateValues: Map<String?, String?>?,
    templateString: String?,
  ): String = getInterpolator().render(templateString, templateValues)

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
  fun getConfigExtractOutput(userConfig: JsonNode?): List<String> {
    val extractOutputConfig =
      Jsons.deserializeToStringList(
        Jsons.getNodeOrEmptyObject(userConfig, EXTRACT_OUTPUT_KEY),
      )

    // match the default BaseOAuth2Flow behaviour, returning ["refresh_token"] by default.
    return if (!extractOutputConfig.isEmpty()) extractOutputConfig else listOf(REFRESH_TOKEN)
  }

  /**
   * Renders the access token parameters by replacing placeholders in the parameter keys and values
   * with the corresponding values from the provided template values.
   *
   * @param templateValues a map containing the template values to be used for rendering the access
   * token parameters
   * @param userConfig a JsonNode containing the user configuration, which includes the access token
   * parameters
   * @return a map with the rendered access token parameters
   * @throws RuntimeException if an IOException occurs during the rendering of the string templates
   */
  internal fun renderConfigAccessTokenParams(
    templateValues: Map<String?, String?>?,
    userConfig: JsonNode?,
  ): Map<String, String> {
    val accessTokenParamsRendered: MutableMap<String, String> = HashMap()
    val userAccessTokenParams =
      Jsons.deserializeToStringMap(
        Jsons.getNodeOrEmptyObject(userConfig, ACCESS_TOKEN_PARAMS_KEY),
      )

    userAccessTokenParams.forEach { (paramKey: String?, paramValue: String?) ->
      try {
        accessTokenParamsRendered[renderStringTemplate(templateValues, paramKey)] = renderStringTemplate(templateValues, paramValue)
      } catch (e: IOException) {
        throw RuntimeException(e)
      }
    }

    return accessTokenParamsRendered
  }

  /**
   * Retrieves and interpolates access token headers from the provided configuration.
   *
   * @param userConfig the JSON configuration node containing access token headers.
   * @return a map of interpolated access token headers.
   * @throws IOException if an I/O error occurs during interpolation
   */
  @Throws(IOException::class)
  fun renderCompleteOAuthHeaders(
    templateValues: Map<String?, String?>?,
    userConfig: JsonNode?,
  ): Map<String, String> {
    val accessTokenHeadersRendered: MutableMap<String, String> = HashMap()
    val userHeaders = Jsons.deserializeToStringMap(Jsons.getNodeOrEmptyObject(userConfig, ACCESS_TOKEN_HEADERS_KEY))

    userHeaders.forEach { (headerKey: String?, headerValue: String?) ->
      try {
        accessTokenHeadersRendered[renderStringTemplate(templateValues, headerKey)] = renderStringTemplate(templateValues, headerValue)
      } catch (e: IOException) {
        throw RuntimeException(e)
      }
    }

    return accessTokenHeadersRendered
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
  @Throws(IOException::class)
  fun processOAuthOutput(
    userConfig: JsonNode,
    data: JsonNode,
    accessTokenUrl: String?,
  ): Map<String, Any> {
    val oauthOutput: MutableMap<String, Any> = HashMap()
    val expectedOAuthOutputFields = getConfigExtractOutput(userConfig)

    for (path in expectedOAuthOutputFields) {
      val value = JsonPaths.getSingleValueTextOrNull(data, path)
      val key = JsonPaths.getTargetKeyFromJsonPath(path)

      if (value != null) {
        // handle `token_expiry_key`
        if (getTokenExpiryKey(userConfig) == key) {
          oauthOutput[TOKEN_EXPIRY_DATE_KEY] = processExpiresIn(value)
        }

        oauthOutput[key] = value
      } else {
        val message = "Missing '%s' field in the `OAuth Output`. Expected fields: %s. Response data: %s"
        throw IOException(String.format(message, key, expectedOAuthOutputFields, data))
      }
    }

    return oauthOutput
  }

  /**
   * Processes the expiration time by adding the specified number of seconds to the current time.
   *
   * @param value the number of seconds to add to the current time, represented as a string
   * @return a string representation of the new expiration time
   */
  private fun processExpiresIn(value: String): String = Instant.now(clock).plusSeconds(value.toInt().toLong()).toString()

  /**
   * Constructs a reference string by formatting the given key and value.
   *
   * @param key the key to be included in the reference string
   * @param value the value to be included in the reference string
   * @return a formatted string in the form "key=value"
   */
  private fun makeParameter(
    key: String?,
    value: String?,
  ): String = String.format("%s=%s", key, value)

  companion object {
    /**
     * The Airbyte Protocol declared literals for an easy access and reuse.
     */
    const val ACCESS_TOKEN: String = "access_token"
    const val ACCESS_TOKEN_HEADERS_KEY: String = "access_token_headers"
    const val ACCESS_TOKEN_KEY: String = "access_token_key"
    const val ACCESS_TOKEN_PARAMS_KEY: String = "access_token_params"
    const val ACCESS_TOKEN_URL: String = "access_token_url"
    const val AUTH_CODE_KEY: String = "auth_code_key"
    const val AUTH_CODE_PARAM: String = "auth_code_param"
    const val AUTH_CODE_VALUE: String = "code"
    const val AUTH_CODE_VALUE_KEY: String = "auth_code_value"
    const val CLIENT_ID_KEY: String = "client_id_key"
    const val CLIENT_ID_PARAM: String = "client_id_param"
    const val CLIENT_ID_VALUE: String = "client_id"
    const val CLIENT_ID_VALUE_KEY: String = "client_id_value"
    const val CLIENT_SECRET_KEY: String = "client_secret_key"
    const val CLIENT_SECRET_PARAM: String = "client_secret_param"
    const val CLIENT_SECRET_VALUE: String = "client_secret"
    const val CLIENT_SECRET_VALUE_KEY: String = "client_secret_value"
    const val CONSENT_URL: String = "consent_url"
    const val EXTRACT_OUTPUT_KEY: String = "extract_output"
    const val REDIRECT_URI_KEY: String = "redirect_uri_key"
    const val REDIRECT_URI_PARAM: String = "redirect_uri_param"
    const val REDIRECT_URI_VALUE: String = "redirect_uri"
    const val REDIRECT_URI_VALUE_KEY: String = "redirect_uri_value"
    const val REFRESH_TOKEN: String = "refresh_token"
    const val SCOPE_KEY: String = "scope_key"
    const val SCOPE_PARAM: String = "scope_param"
    const val SCOPE_VALUE: String = "scope"
    const val SCOPE_VALUE_KEY: String = "scope_value"
    const val STATE_KEY: String = "state_key"
    const val STATE_PARAM: String = "state_param"
    const val STATE_VALUE: String = "state"
    const val STATE_VALUE_KEY: String = "state_value"
    const val STATE_PARAM_KEY: String = STATE_VALUE
    const val STATE_PARAM_MIN_KEY: String = "min"
    const val STATE_PARAM_MAX_KEY: String = "max"
    const val TOKEN_EXPIRY_KEY: String = "token_expiry_key"
    const val TOKEN_EXPIRY_VALUE: String = "expires_in"
    const val TOKEN_EXPIRY_DATE_KEY: String = "token_expiry_date"

    /**
     * The minimum and maximum length for the OAuth state parameter. This values is used to ensure that
     * the state parameter has a sufficient length, acceptable by data-provider.
     */
    private const val STATE_LEN_MIN = 7
    private const val STATE_LEN_MAX = 24

    /**
     * A string containing the characters allowed in the state parameter for OAuth. This includes
     * lowercase and uppercase letters, digits, and the characters '.', '-', and '_'.
     */
    private const val STATE_ALLOWED_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_"

    /**
     * A map containing the mandatory default keys used in OAuth specifications. These keys are
     * essential for the OAuth process and are mapped to their respective default values.
     *
     */
    private val MANDATORY_DEFAULT_KEYS: Map<String, String> =
      java.util.Map.of(
        AUTH_CODE_KEY,
        AUTH_CODE_VALUE,
        CLIENT_ID_KEY,
        CLIENT_ID_VALUE,
        CLIENT_SECRET_KEY,
        CLIENT_SECRET_VALUE,
        REDIRECT_URI_KEY,
        REDIRECT_URI_VALUE,
        SCOPE_KEY,
        SCOPE_VALUE,
        STATE_KEY,
        STATE_VALUE,
      )

    /**
     * Creates and returns a new instance of Jinjava with a custom filter registered. The custom filter
     * `codeChallengeS256` is registered to the Jinjava instance's global context.
     *
     * @return a Jinjava instance with the `codeChallengeS256` filter registered.
     */
    private fun getInterpolator(): Jinjava {
      val interpolator = Jinjava()
      // register the `codeChallengeS256` filter
      interpolator.globalContext.registerFilter(CodeChallengeS256Filter())

      return interpolator
    }

    /**
     * Encodes the given string using the UTF-8 encoding scheme.
     *
     * @param s the string to be encoded; if null, the method returns null
     * @return the encoded string, or null if the input string is null
     * @throws RuntimeException if the encoding process fails
     */
    private fun urlEncode(s: String?): String? {
      if (s == null) {
        return s
      }

      try {
        return URLEncoder.encode(s, StandardCharsets.UTF_8)
      } catch (e: Exception) {
        throw RuntimeException(e)
      }
    }
  }
}
