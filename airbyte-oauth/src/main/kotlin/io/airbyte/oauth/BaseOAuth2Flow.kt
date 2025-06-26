/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.collect.ImmutableMap
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.random.randomAlpha
import io.airbyte.config.ConfigSchema
import io.airbyte.protocol.models.v0.OAuthConfigSpecification
import io.airbyte.validation.json.JsonSchemaValidator
import io.airbyte.validation.json.JsonValidationException
import java.io.IOException
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.function.Function
import java.util.function.Supplier

/**
 * Abstract Class factoring common behavior for oAuth 2.0 flow implementations.
 */
abstract class BaseOAuth2Flow
  @JvmOverloads
  constructor(
    @JvmField protected val httpClient: HttpClient,
    private val stateSupplier: Supplier<String> = Supplier { generateRandomState() },
    @JvmField protected val tokenReqContentType: TokenRequestContentType = TokenRequestContentType.URL_ENCODED,
  ) : BaseOAuthFlow() {
    /**
     * Simple enum of content type strings and their respective encoding functions used for POSTing the
     * access token request.
     */
    enum class TokenRequestContentType(
      /**
       * Get HTTP content type.
       *
       * @return content type
       */
      @JvmField var contentType: String,
      /**
       * Get converter to json.
       *
       * @return converter function
       */
      @JvmField var converter: Function<Map<String, String>, String>,
    ) {
      URL_ENCODED(
        "application/x-www-form-urlencoded",
        Function { body: Map<String, String> -> toUrlEncodedString(body) },
      ),
      JSON(
        "application/json",
        Function { body: Map<String, String>? -> toJson(body) },
      ),
    }

    // possible errors enumerated @
    // https://www.oauth.com/oauth2-servers/server-side-apps/possible-errors/
    private val ignoredOauthErrors = listOf("access_denied")

    /**
     * Retrieves the content type to be used for the token request.
     *
     * @param inputOAuthConfiguration the OAuth configuration as a JsonNode
     * @return the content type for the token request, which is URL_ENCODED by default
     */
    protected open fun getRequestContentType(inputOAuthConfiguration: JsonNode?): TokenRequestContentType = TokenRequestContentType.URL_ENCODED

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
    @Throws(IOException::class, JsonValidationException::class)
    override fun getSourceConsentUrl(
      workspaceId: UUID,
      sourceDefinitionId: UUID?,
      redirectUrl: String,
      inputOAuthConfiguration: JsonNode,
      oauthConfigSpecification: OAuthConfigSpecification?,
      sourceOAuthParamConfig: JsonNode?,
    ): String {
      validateInputOAuthConfiguration(oauthConfigSpecification, inputOAuthConfiguration)
      // This should probably never happen because the caller of this function should throw this exception
      // when fetching the param, but this was the prior behavior so adding it here.
      if (sourceOAuthParamConfig == null) {
        throw ResourceNotFoundProblem(
          "Undefined OAuth Parameter.",
          ProblemResourceData().resourceType(ConfigSchema.SOURCE_OAUTH_PARAM.name),
        )
      }

      return formatConsentUrl(
        sourceDefinitionId,
        getClientIdUnsafe(sourceOAuthParamConfig),
        redirectUrl,
        Jsons.mergeNodes(inputOAuthConfiguration, getOAuthDeclarativeInputSpec(oauthConfigSpecification)),
      )
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
    @Throws(IOException::class, JsonValidationException::class)
    override fun getDestinationConsentUrl(
      workspaceId: UUID,
      destinationDefinitionId: UUID?,
      redirectUrl: String,
      inputOAuthConfiguration: JsonNode,
      oauthConfigSpecification: OAuthConfigSpecification?,
      destinationOAuthParamConfig: JsonNode?,
    ): String {
      validateInputOAuthConfiguration(oauthConfigSpecification, inputOAuthConfiguration)
      // This should probably never happen because the caller of this function should throw this exception
      // when fetching the param, but this was the prior behavior so adding it here.
      if (destinationOAuthParamConfig == null) {
        throw ResourceNotFoundProblem(
          "Undefined OAuth Parameter.",
          ProblemResourceData().resourceType(ConfigSchema.DESTINATION_OAUTH_PARAM.name),
        )
      }

      return formatConsentUrl(
        destinationDefinitionId,
        getClientIdUnsafe(destinationOAuthParamConfig),
        redirectUrl,
        Jsons.mergeNodes(inputOAuthConfiguration, getOAuthDeclarativeInputSpec(oauthConfigSpecification)),
      )
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
     * Flow
     */
    @Throws(IOException::class)
    protected abstract fun formatConsentUrl(
      definitionId: UUID?,
      clientId: String,
      redirectUrl: String,
      inputOAuthConfiguration: JsonNode,
    ): String

    /**
     * Generate a string to use as state in the OAuth process.
     */
    protected open fun getState(): String = stateSupplier.get()

    // Overload method to provide the `inputOAuthConfiguration` spec config.
    protected open fun getState(inputOAuthConfiguration: JsonNode): String = getState()

    @Deprecated("")
    @Throws(IOException::class)
    override fun completeSourceOAuth(
      workspaceId: UUID,
      sourceDefinitionId: UUID?,
      queryParams: Map<String, Any>,
      redirectUrl: String,
      oauthParamConfig: JsonNode,
    ): Map<String, Any> {
      if (containsIgnoredOAuthError(queryParams)) {
        return buildRequestError(queryParams)
      }
      return formatOAuthOutput(
        oauthParamConfig,
        completeOAuthFlow(
          getClientIdUnsafe(oauthParamConfig),
          getClientSecretUnsafe(oauthParamConfig),
          extractCodeParameter(queryParams)!!,
          redirectUrl,
          Jsons.emptyObject(),
          oauthParamConfig,
          null,
        ),
        getDefaultOAuthOutputPath(),
      )
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
    @Throws(IOException::class, JsonValidationException::class)
    override fun completeSourceOAuth(
      workspaceId: UUID,
      sourceDefinitionId: UUID?,
      queryParams: Map<String, Any>,
      redirectUrl: String,
      inputOAuthConfiguration: JsonNode,
      oauthConfigSpecification: OAuthConfigSpecification,
      oauthParamConfig: JsonNode,
    ): Map<String, Any> {
      validateInputOAuthConfiguration(oauthConfigSpecification, inputOAuthConfiguration)
      if (containsIgnoredOAuthError(queryParams)) {
        return buildRequestError(queryParams)
      }

      val oauthConfigurationMerged = Jsons.mergeNodes(inputOAuthConfiguration, getOAuthDeclarativeInputSpec(oauthConfigSpecification))
      return formatOAuthOutput(
        oauthParamConfig,
        completeOAuthFlow(
          getClientIdUnsafe(oauthParamConfig),
          getClientSecretUnsafe(oauthParamConfig),
          extractCodeParameter(queryParams)!!,
          redirectUrl,
          oauthConfigurationMerged,
          oauthParamConfig,
          extractStateParameter(queryParams, oauthConfigurationMerged),
        ),
        oauthConfigSpecification,
      )
    }

    @Deprecated("")
    @Throws(IOException::class)
    override fun completeDestinationOAuth(
      workspaceId: UUID,
      destinationDefinitionId: UUID?,
      queryParams: Map<String, Any>,
      redirectUrl: String,
      oauthParamConfig: JsonNode,
    ): Map<String, Any> {
      if (containsIgnoredOAuthError(queryParams)) {
        return buildRequestError(queryParams)
      }
      return formatOAuthOutput(
        oauthParamConfig,
        completeOAuthFlow(
          getClientIdUnsafe(oauthParamConfig),
          getClientSecretUnsafe(oauthParamConfig),
          extractCodeParameter(queryParams)!!,
          redirectUrl,
          Jsons.emptyObject(),
          oauthParamConfig,
          null,
        ),
        getDefaultOAuthOutputPath(),
      )
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
    @Throws(IOException::class, JsonValidationException::class)
    override fun completeDestinationOAuth(
      workspaceId: UUID,
      destinationDefinitionId: UUID?,
      queryParams: Map<String, Any>,
      redirectUrl: String,
      inputOAuthConfiguration: JsonNode,
      oauthConfigSpecification: OAuthConfigSpecification,
      oauthParamConfig: JsonNode,
    ): Map<String, Any> {
      validateInputOAuthConfiguration(oauthConfigSpecification, inputOAuthConfiguration)
      if (containsIgnoredOAuthError(queryParams)) {
        return buildRequestError(queryParams)
      }

      val oauthConfigurationMerged = Jsons.mergeNodes(inputOAuthConfiguration, getOAuthDeclarativeInputSpec(oauthConfigSpecification))
      return formatOAuthOutput(
        oauthParamConfig,
        completeOAuthFlow(
          getClientIdUnsafe(oauthParamConfig),
          getClientSecretUnsafe(oauthParamConfig),
          extractCodeParameter(queryParams)!!,
          redirectUrl,
          oauthConfigurationMerged,
          oauthParamConfig,
          extractStateParameter(queryParams, oauthConfigurationMerged),
        ),
        oauthConfigSpecification,
      )
    }

    /**
     * Generates the headers required for completing the OAuth flow request.
     *
     * @param inputOAuthConfiguration the JSON node containing the OAuth configuration.
     * @return a map containing the headers for the OAuth flow request.
     * @throws IOException if an I/O error occurs.
     */
    @Throws(IOException::class)
    protected open fun getCompleteOAuthFlowRequestHeaders(
      clientId: String?,
      clientSecret: String?,
      authCode: String?,
      redirectUrl: String?,
      inputOAuthConfiguration: JsonNode,
    ): Map<String, String> {
      val requestHeaders: MutableMap<String, String> = HashMap()
      requestHeaders["Content-Type"] = tokenReqContentType.contentType
      requestHeaders["Accept"] = "application/json"

      return requestHeaders
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
    @Throws(IOException::class)
    protected fun getCompleteOAuthFlowHttpRequest(
      accessTokenUrl: String,
      accessTokenQueryParameters: Map<String, String>,
      requestHeaders: Map<String, String>,
      requestContentType: TokenRequestContentType,
      inputOAuthConfiguration: JsonNode?,
    ): HttpRequest {
      // prepare query params
      val contentTypeConverter =
        requestContentType.converter
      val contentTypeConvertedParams = contentTypeConverter.apply(accessTokenQueryParameters)

      // prepare the request
      val request = HttpRequest.newBuilder()
      request.POST(HttpRequest.BodyPublishers.ofString(contentTypeConvertedParams))
      request.uri(URI.create(accessTokenUrl))
      requestHeaders.forEach { (name: String?, value: String?) -> request.header(name, value) }

      // build the request
      return request.build()
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
    @Throws(IOException::class)
    protected open fun completeOAuthFlow(
      clientId: String,
      clientSecret: String,
      authCode: String,
      redirectUrl: String,
      inputOAuthConfiguration: JsonNode,
      oauthParamConfig: JsonNode,
      state: String?,
    ): Map<String, Any> =
      getCompleteOAuthFlowOutput(
        formatAccessTokenUrl(
          getAccessTokenUrl(inputOAuthConfiguration),
          clientId,
          clientSecret,
          authCode,
          redirectUrl,
          inputOAuthConfiguration,
          state,
        ),
        getAccessTokenQueryParameters(clientId, clientSecret, authCode, redirectUrl, state, inputOAuthConfiguration),
        getCompleteOAuthFlowRequestHeaders(clientId, clientSecret, authCode, redirectUrl, inputOAuthConfiguration),
        getRequestContentType(inputOAuthConfiguration),
        inputOAuthConfiguration,
      )

    @Throws(IOException::class)
    protected fun getCompleteOAuthFlowOutput(
      accessTokenUrl: String,
      accessTokenQueryParameters: Map<String, String>,
      requestHeaders: Map<String, String>,
      requestContentType: TokenRequestContentType,
      inputOAuthConfiguration: JsonNode,
    ): Map<String, Any> {
      val request =
        getCompleteOAuthFlowHttpRequest(
          accessTokenUrl,
          accessTokenQueryParameters,
          requestHeaders,
          requestContentType,
          inputOAuthConfiguration,
        )

      try {
        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
        return extractOAuthOutput(Jsons.deserialize(response.body()), accessTokenUrl, inputOAuthConfiguration)
      } catch (e: InterruptedException) {
        throw IOException("Failed to complete OAuth flow", e)
      }
    }

    /**
     * Query parameters to provide the access token url with.
     */
    protected open fun getAccessTokenQueryParameters(
      clientId: String,
      clientSecret: String,
      authCode: String,
      redirectUrl: String,
      state: String?,
      inputOAuthConfiguration: JsonNode,
    ): Map<String, String> = getAccessTokenQueryParameters(clientId, clientSecret, authCode, redirectUrl)

    /**
     * Query parameters to provide the access token url with.
     */
    protected open fun getAccessTokenQueryParameters(
      clientId: String,
      clientSecret: String,
      authCode: String,
      redirectUrl: String,
    ): Map<String, String> =
      ImmutableMap
        .builder<String, String>() // required
        .put("client_id", clientId)
        .put("redirect_uri", redirectUrl)
        .put("client_secret", clientSecret)
        .put("code", authCode)
        .build()

    /**
     * Once the user is redirected after getting their consent, the API should redirect them to a
     * specific redirection URL along with query parameters. This function should parse and extract the
     * code from these query parameters in order to continue the OAuth Flow.
     */
    @Throws(IOException::class)
    protected open fun extractCodeParameter(queryParams: Map<String, Any>): String {
      if (queryParams.containsKey("code")) {
        return queryParams["code"] as String
      } else {
        throw IOException("Undefined 'code' from consent redirected url.")
      }
    }

    /**
     * If there is an error param, return it.
     */
    protected fun extractErrorParameter(queryParams: Map<String, Any>): String? =
      if (queryParams.containsKey("error")) {
        queryParams["error"] as String?
      } else {
        null
      }

    protected fun containsIgnoredOAuthError(queryParams: Map<String, Any>): Boolean {
      val oauthError = extractErrorParameter(queryParams)
      return oauthError != null && ignoredOauthErrors.contains(oauthError)
    }

    /**
     * Return an error payload if the previous oAuth request was stopped.
     */
    protected fun buildRequestError(queryParams: Map<String, Any>): Map<String, Any> {
      val results: MutableMap<String, Any> = HashMap()
      results["request_succeeded"] = false
      results["request_error"] = extractErrorParameter(queryParams)!!
      return results
    }

    /**
     * An Overload to return the accessTokenUrl with additional customisation.
     */
    protected abstract fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String

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
    @Throws(IOException::class)
    protected fun formatAccessTokenUrl(
      accessTokenUrl: String,
      clientId: String?,
      clientSecret: String?,
      authCode: String?,
      redirectUrl: String?,
      inputOAuthConfiguration: JsonNode?,
    ): String = accessTokenUrl

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
    @Throws(IOException::class)
    protected open fun formatAccessTokenUrl(
      accessTokenUrl: String,
      clientId: String?,
      clientSecret: String?,
      authCode: String?,
      redirectUrl: String?,
      inputOAuthConfiguration: JsonNode,
      state: String?,
    ): String = formatAccessTokenUrl(accessTokenUrl, clientId, clientSecret, authCode, redirectUrl, inputOAuthConfiguration)

    /**
     * Extract all OAuth outputs from distant API response and store them in a flat map.
     */
    @Throws(IOException::class)
    protected open fun extractOAuthOutput(
      data: JsonNode,
      accessTokenUrl: String,
      inputOAuthConfiguration: JsonNode,
    ): Map<String, Any> = extractOAuthOutput(data, accessTokenUrl)

    /**
     * Extract all OAuth outputs from distant API response and store them in a flat map.
     */
    @Throws(IOException::class)
    protected open fun extractOAuthOutput(
      data: JsonNode,
      accessTokenUrl: String,
    ): Map<String, Any> {
      val result: MutableMap<String, Any> = HashMap()
      if (data.has("refresh_token")) {
        result["refresh_token"] = data["refresh_token"].asText()
      } else {
        throw IOException(String.format("Missing 'refresh_token' in query params from %s", accessTokenUrl))
      }
      return result
    }

    /**
     * This function should parse and extract the state from these query parameters in order to continue
     * the OAuth Flow.
     */
    @Throws(IOException::class)
    protected open fun extractStateParameter(
      queryParams: Map<String, Any>,
      inputOAuthConfiguration: JsonNode?,
    ): String? =
      if (queryParams.containsKey("state")) {
        queryParams["state"] as String?
      } else {
        null
      }

    @Deprecated("")
    override fun getDefaultOAuthOutputPath(): List<String> = listOf("credentials")

    companion object {
      private fun generateRandomState(): String = randomAlpha(7)

      @JvmStatic
      @Throws(JsonValidationException::class)
      protected fun validateInputOAuthConfiguration(
        oauthConfigSpecification: OAuthConfigSpecification?,
        inputOAuthConfiguration: JsonNode?,
      ) {
        if (oauthConfigSpecification != null && oauthConfigSpecification.oauthUserInputFromConnectorConfigSpecification != null) {
          val validator = JsonSchemaValidator()
          validator.ensure(oauthConfigSpecification.oauthUserInputFromConnectorConfigSpecification, inputOAuthConfiguration)
        }
      }

      private fun urlEncode(s: String): String {
        try {
          return URLEncoder.encode(s, StandardCharsets.UTF_8)
        } catch (e: Exception) {
          throw RuntimeException(e)
        }
      }

      private fun toUrlEncodedString(body: Map<String, String>): String {
        val result = StringBuilder()
        for ((key, value) in body) {
          if (result.length > 0) {
            result.append("&")
          }
          result.append(key).append("=").append(urlEncode(value))
        }
        return result.toString()
      }

      protected fun toJson(body: Map<String, String>?): String {
        val gson = Gson()
        val gsonType = object : TypeToken<Map<String?, String?>?>() {}.type
        return gson.toJson(body, gsonType)
      }
    }
  }
