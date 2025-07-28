/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.declarative

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.oauth.BaseOAuth2Flow
import io.airbyte.protocol.models.v0.OAuthConfigSpecification
import io.airbyte.validation.json.JsonValidationException
import org.apache.http.client.utils.URIBuilder
import java.io.IOException
import java.net.URISyntaxException
import java.net.http.HttpClient
import java.util.UUID
import java.util.function.Supplier

class DeclarativeOAuthFlow : BaseOAuth2Flow {
  @JvmField
  val specHandler: DeclarativeOAuthSpecHandler = DeclarativeOAuthSpecHandler()

  constructor(httpClient: HttpClient) : super(httpClient)

  @VisibleForTesting
  constructor(httpClient: HttpClient, stateSupplier: Supplier<String>) : super(httpClient, stateSupplier)

  /**
   * Validates the input OAuth configuration against the provided OAuth configuration specification.
   * Additionally, checks if the OAuth parameter configuration is defined.
   *
   * @param oauthConfigSpecification the specification against which the input OAuth configuration is
   * validated
   * @param inputOAuthConfiguration the input OAuth configuration to be validated
   * @param oauthParamConfig the OAuth parameter configuration to be checked for null
   * @throws IOException if an I/O error occurs during validation
   * @throws JsonValidationException if the input OAuth configuration does not conform to the
   * specification
   * @throws ResourceNotFoundProblem if the OAuth parameter configuration is null
   */
  @Throws(IOException::class, JsonValidationException::class)
  protected fun validateInputOAuthConfiguration(
    oauthConfigSpecification: OAuthConfigSpecification?,
    inputOAuthConfiguration: JsonNode?,
    oauthParamConfig: JsonNode?,
  ) {
    validateInputOAuthConfiguration(oauthConfigSpecification, inputOAuthConfiguration)
    if (oauthParamConfig == null) {
      val problem = ProblemResourceData().resourceType(ConfigNotFoundType.SOURCE_OAUTH_PARAM.name)
      throw ResourceNotFoundProblem("Undefined OAuth Parameter.", problem)
    }
  }

  /**
   * Overrides the getState method to provide a customizable STATE parameter.
   *
   * @param inputOAuthConfiguration the JSON node containing OAuth configuration details.
   * @return a customizable STATE parameter as a String.
   */
  override fun getState(inputOAuthConfiguration: JsonNode): String {
    val state = inputOAuthConfiguration.path(DeclarativeOAuthSpecHandler.STATE_PARAM_KEY)!!
    return if (state.isMissingNode) getState() else specHandler.getConfigurableState(state)
  }

  /**
   * Generates the source consent URL for OAuth authentication.
   *
   * @param workspaceId the UUID of the workspace.
   * @param sourceDefinitionId the UUID of the source definition.
   * @param redirectUrl the URL to redirect to after consent is granted.
   * @param inputOAuthConfiguration the input OAuth configuration as a JsonNode.
   * @param oauthConfigSpecification the OAuth configuration specification.
   * @param sourceOAuthParamConfig the source OAuth parameter configuration as a JsonNode.
   * @return the formatted consent URL as a String.
   * @throws IOException if an I/O error occurs.
   * @throws JsonValidationException if the JSON validation fails.
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
    validateInputOAuthConfiguration(oauthConfigSpecification, inputOAuthConfiguration, sourceOAuthParamConfig)
    val declarativeOAuthConfig = getOAuthDeclarativeInputSpec(oauthConfigSpecification)
    return formatConsentUrl(
      sourceDefinitionId,
      getConfigValueUnsafe(sourceOAuthParamConfig, specHandler.getClientIdKey(declarativeOAuthConfig)),
      redirectUrl,
      Jsons.mergeNodes(inputOAuthConfiguration, declarativeOAuthConfig),
    )
  }

  /**
   * Generates the destination consent URL for OAuth authentication.
   *
   * @param workspaceId the UUID of the workspace
   * @param destinationDefinitionId the UUID of the destination definition
   * @param redirectUrl the URL to redirect to after consent is granted
   * @param inputOAuthConfiguration the input OAuth configuration as a JsonNode
   * @param oauthConfigSpecification the OAuth configuration specification
   * @param destinationOAuthParamConfig the destination OAuth parameter configuration as a JsonNode
   * @return the formatted consent URL as a String
   * @throws IOException if an I/O error occurs
   * @throws JsonValidationException if the JSON validation fails
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
    validateInputOAuthConfiguration(oauthConfigSpecification, inputOAuthConfiguration, destinationOAuthParamConfig)
    val declarativeOAuthConfig = getOAuthDeclarativeInputSpec(oauthConfigSpecification)
    return formatConsentUrl(
      destinationDefinitionId,
      getConfigValueUnsafe(destinationOAuthParamConfig, specHandler.getClientIdKey(declarativeOAuthConfig)),
      redirectUrl,
      Jsons.mergeNodes(inputOAuthConfiguration, declarativeOAuthConfig),
    )
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
  @Throws(IOException::class)
  override fun formatConsentUrl(
    definitionId: UUID?,
    clientId: String,
    redirectUrl: String,
    inputOAuthConfiguration: JsonNode,
  ): String {
    val consentUrlRendered =
      specHandler.renderStringTemplate(
        specHandler.getConsentUrlTemplateValues(
          inputOAuthConfiguration,
          clientId,
          redirectUrl,
          getState(inputOAuthConfiguration),
        ),
        getConfigValueUnsafe(inputOAuthConfiguration, DeclarativeOAuthSpecHandler.CONSENT_URL),
      )

    try {
      return URIBuilder(consentUrlRendered).build().toString()
    } catch (e: URISyntaxException) {
      throw IOException("Failed to format Consent URL for OAuth flow.", e)
    }
  }

  /**
   * Determines the token request content type based on the provided OAuth configuration.
   *
   * @param inputOAuthConfiguration the JSON node representing the OAuth configuration.
   * @return the determined `TokenRequestContentType` for the access token request.
   */
  override fun getRequestContentType(inputOAuthConfiguration: JsonNode?): TokenRequestContentType {
    inputOAuthConfiguration!!
    val headers = inputOAuthConfiguration.path(DeclarativeOAuthSpecHandler.ACCESS_TOKEN_HEADERS_KEY)
    val params = inputOAuthConfiguration.path(DeclarativeOAuthSpecHandler.ACCESS_TOKEN_PARAMS_KEY)

    // When no 'access_token_params' are provided, fall back to the default
    // `application/x-www-form-urlencoded`.
    if (params.isMissingNode) {
      return super.getRequestContentType(inputOAuthConfiguration)
    }

    // If headers exist, try to extract the 'Content-Type'.
    if (!headers.isMissingNode) {
      val contentTypeNode = headers["Content-Type"]

      if (contentTypeNode != null) {
        val contentType = contentTypeNode.asText()

        // application/x-www-form-urlencoded
        if (TokenRequestContentType.URL_ENCODED.contentType == contentType) {
          return TokenRequestContentType.URL_ENCODED
        }
        // application/json
        if (TokenRequestContentType.JSON.contentType == contentType) {
          return TokenRequestContentType.JSON
        }
      }
    }

    // Default to `application/json` when params exist but no recognizable 'Content-Type' is specified.
    return TokenRequestContentType.JSON
  }

  /**
   * IMPORTANT: DO NOT MODIFY!
   *
   * Generates the access token query parameters required for OAuth authentication.
   *
   * @param clientId The client ID provided by the OAuth provider.
   * @param clientSecret The client secret provided by the OAuth provider.
   * @param authCode The authorization code received from the OAuth provider after user authorization.
   * @param redirectUrl The redirect URL configured for the OAuth provider.
   * @param state The state parameter to maintain state between the request and callback.
   * @param inputOAuthConfiguration The JSON configuration containing additional OAuth parameters.
   * @return A map containing the rendered access token query parameters or an empty HashMap.
   */
  override fun getAccessTokenQueryParameters(
    clientId: String,
    clientSecret: String,
    authCode: String,
    redirectUrl: String,
    state: String?,
    inputOAuthConfiguration: JsonNode,
  ): Map<String, String> {
    val renderedAccessTokenQueryParams =
      specHandler.renderConfigAccessTokenParams(
        specHandler.getAccessTokenParamsTemplateValues(
          inputOAuthConfiguration,
          clientId,
          clientSecret,
          authCode,
          redirectUrl,
          state,
        ),
        inputOAuthConfiguration,
      )

    return renderedAccessTokenQueryParams
  }

  /**
   * IMPORTANT: DO NOT MODIFY!
   *
   * Retrieves the access token URL from the provided OAuth configuration.
   *
   * @param inputOAuthConfiguration the JSON node containing the OAuth configuration.
   * @return the access token URL.
   */
  override fun getAccessTokenUrl(inputOAuthConfiguration: JsonNode): String =
    getConfigValueUnsafe(inputOAuthConfiguration, DeclarativeOAuthSpecHandler.ACCESS_TOKEN_URL)

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
  @Throws(IOException::class)
  override fun getCompleteOAuthFlowRequestHeaders(
    clientId: String?,
    clientSecret: String?,
    authCode: String?,
    redirectUrl: String?,
    inputOAuthConfiguration: JsonNode,
  ): Map<String, String> {
    val renderedCompleteOAuthHeaders =
      specHandler.renderCompleteOAuthHeaders(
        specHandler.getCompleteOAuthHeadersTemplateValues(
          inputOAuthConfiguration,
          clientId,
          clientSecret,
          authCode,
          redirectUrl,
        ),
        inputOAuthConfiguration,
      )

    return if (!renderedCompleteOAuthHeaders.isEmpty()) {
      renderedCompleteOAuthHeaders
    } else {
      super.getCompleteOAuthFlowRequestHeaders(
        clientId,
        clientSecret,
        authCode,
        redirectUrl,
        inputOAuthConfiguration,
      )
    }
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
  @Throws(IOException::class)
  override fun formatAccessTokenUrl(
    accessTokenUrl: String,
    clientId: String?,
    clientSecret: String?,
    authCode: String?,
    redirectUrl: String?,
    inputOAuthConfiguration: JsonNode,
    state: String?,
  ): String {
    val accessTokenUrlRendered =
      specHandler.renderStringTemplate(
        specHandler.getAccessTokenUrlTemplateValues(
          inputOAuthConfiguration,
          clientId,
          clientSecret,
          authCode,
          redirectUrl,
          state,
        ),
        accessTokenUrl,
      )

    try {
      return URIBuilder(accessTokenUrlRendered).build().toString()
    } catch (e: URISyntaxException) {
      throw IOException("Failed to format AccessToken URL for OAuth flow.", e)
    }
  }

  /**
   * Completes the OAuth flow for a source.
   *
   * @param workspaceId the ID of the workspace
   * @param sourceDefinitionId the ID of the source definition
   * @param queryParams the query parameters from the OAuth callback
   * @param redirectUrl the redirect URL used in the OAuth flow
   * @param inputOAuthConfiguration the initial OAuth configuration provided by the user
   * @param oauthConfigSpecification the specification for the OAuth configuration
   * @param oauthParamConfig the OAuth parameter configuration
   * @return a map containing the completed OAuth configuration
   * @throws IOException if an I/O error occurs
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

    val declarativeOAuthConfig = getOAuthDeclarativeInputSpec(oauthConfigSpecification)
    val oauthConfigurationMerged = Jsons.mergeNodes(inputOAuthConfiguration, declarativeOAuthConfig)

    return formatOAuthOutput(
      oauthParamConfig,
      completeOAuthFlow(
        getConfigValueUnsafe(oauthParamConfig, specHandler.getClientIdKey(declarativeOAuthConfig)),
        getConfigValueUnsafe(oauthParamConfig, specHandler.getClientSecretKey(declarativeOAuthConfig)),
        extractCodeParameter(queryParams, oauthConfigurationMerged)!!,
        redirectUrl,
        oauthConfigurationMerged,
        oauthParamConfig,
        extractStateParameter(queryParams, oauthConfigurationMerged),
      ),
      oauthConfigSpecification,
    )
  }

  /**
   * Completes the OAuth flow for a destination by validating the input OAuth configuration, merging
   * it with the declarative OAuth configuration, and formatting the output.
   *
   * @param workspaceId the ID of the workspace
   * @param destinationDefinitionId the ID of the destination definition
   * @param queryParams the query parameters from the OAuth callback
   * @param redirectUrl the redirect URL used in the OAuth flow
   * @param inputOAuthConfiguration the input OAuth configuration provided by the user
   * @param oauthConfigSpecification the OAuth configuration specification
   * @param oauthParamConfig the OAuth parameter configuration
   * @return a map containing the completed OAuth configuration
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

    val declarativeOAuthConfig = getOAuthDeclarativeInputSpec(oauthConfigSpecification)
    val oauthConfigurationMerged = Jsons.mergeNodes(inputOAuthConfiguration, declarativeOAuthConfig)
    return formatOAuthOutput(
      oauthParamConfig,
      completeOAuthFlow(
        getConfigValueUnsafe(oauthParamConfig, specHandler.getClientIdKey(declarativeOAuthConfig)),
        getConfigValueUnsafe(oauthParamConfig, specHandler.getClientSecretKey(declarativeOAuthConfig)),
        extractCodeParameter(queryParams, oauthConfigurationMerged)!!,
        redirectUrl,
        oauthConfigurationMerged,
        oauthParamConfig,
        extractStateParameter(queryParams, oauthConfigurationMerged),
      ),
      oauthConfigSpecification,
    )
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
  @Throws(IOException::class)
  override fun extractOAuthOutput(
    data: JsonNode,
    accessTokenUrl: String,
    inputOAuthConfiguration: JsonNode,
  ): Map<String, Any> = specHandler.processOAuthOutput(inputOAuthConfiguration, data, accessTokenUrl)

  /**
   * Extracts the state parameter from the query parameters based on the input OAuth configuration.
   *
   * @param queryParams the map of query parameters from the redirect URI
   * @param inputOAuthConfiguration the JSON node containing the OAuth configuration
   * @return the state parameter value if present in the query parameters
   * @throws IOException if the state key is not found in the query parameters
   */
  @Throws(IOException::class)
  protected override fun extractStateParameter(
    queryParams: Map<String, Any>,
    inputOAuthConfiguration: JsonNode?,
  ): String {
    // get the state key name with respect to userConfig input
    val stateKey = specHandler.getStateKey(inputOAuthConfiguration!!)
    if (queryParams.containsKey(stateKey)) {
      return queryParams[stateKey] as String
    } else {
      val errorMsg = String.format("Undefined `state_key`: '%s' from `redirect_uri`. Keys available: %s", stateKey, queryParams.keys)
      throw IOException(errorMsg)
    }
  }

  /**
   * Extracts the authorization code parameter from the provided query parameters.
   *
   * @param queryParams a map containing the query parameters from the redirect URI
   * @param inputOAuthConfiguration a JsonNode containing the OAuth configuration
   * @return the extracted authorization code as a String
   * @throws IOException if the authorization code key is not found in the query parameters
   */
  @Throws(IOException::class)
  protected fun extractCodeParameter(
    queryParams: Map<String, Any>,
    inputOAuthConfiguration: JsonNode,
  ): String? {
    // get the auth code key name with respect to userConfig input
    val authCodeKey = specHandler.getAuthCodeKey(inputOAuthConfiguration)
    if (queryParams.containsKey(authCodeKey)) {
      return queryParams[authCodeKey] as String?
    } else {
      val errorMsg = String.format("Undefined `auth_code_key`: '%s' from `redirect_uri`. Keys available: %s", authCodeKey, queryParams.keys)
      throw IOException(errorMsg)
    }
  }
}
