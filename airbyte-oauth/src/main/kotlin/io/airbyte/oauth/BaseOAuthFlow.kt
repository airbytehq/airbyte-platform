/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.collect.ImmutableMap
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.map.MoreMaps
import io.airbyte.protocol.models.v0.OAuthConfigSpecification
import io.airbyte.validation.json.JsonSchemaValidator
import io.airbyte.validation.json.JsonValidationException
import java.util.function.BiConsumer

/**
 * Abstract Class implementing common base methods for managing oAuth config (instance-wide) and
 * oAuth specifications.
 */
abstract class BaseOAuthFlow : OAuthFlowImplementation {
  /**
   * Throws an exception if the client ID cannot be extracted. Subclasses should override this to
   * parse the config differently.
   *
   * @return The configured Client ID used for this oauth flow
   */
  protected open fun getClientIdUnsafe(oauthConfig: JsonNode): String = getConfigValueUnsafe(oauthConfig, "client_id")

  /**
   * Throws an exception if the client secret cannot be extracted. Subclasses should override this to
   * parse the config differently.
   *
   * @return The configured client secret for this OAuthFlow
   */
  protected open fun getClientSecretUnsafe(oauthConfig: JsonNode): String = getConfigValueUnsafe(oauthConfig, "client_secret")

  /**
   * completeOAuth calls should output a flat map of fields produced by the oauth flow to be forwarded
   * back to the connector config. This @deprecated function is used when the connector's oauth
   * specifications are unknown. So it ends up using hard-coded output path in the OAuth Flow
   * implementation instead of relying on the connector's specification to determine where the outputs
   * should be stored.
   */
  @Deprecated("")
  protected fun formatOAuthOutput(
    oauthParamConfig: JsonNode,
    oauthOutput: Map<String, Any>,
    outputPath: List<String>,
  ): Map<String, Any> {
    var result: MutableMap<String, Any> = HashMap(oauthOutput)
    for (key in Jsons.keys(oauthParamConfig)) {
      result[key] = MoreOAuthParameters.SECRET_MASK
    }
    for (node in outputPath) {
      result = java.util.Map.of(node, result)
    }
    return result
  }

  /**
   * completeOAuth calls should output a flat map of fields produced by the oauth flow to be forwarded
   * back to the connector config. This function follows the connector's oauth specifications of which
   * outputs are expected and filters them accordingly.
   */
  @Throws(JsonValidationException::class)
  protected fun formatOAuthOutput(
    oauthParamConfig: JsonNode,
    completeOAuthFlow: Map<String, Any>,
    oauthConfigSpecification: OAuthConfigSpecification,
  ): Map<String, Any> {
    val validator = JsonSchemaValidator()

    val oAuthOutputs: Map<String, Any> =
      formatOAuthOutput(
        validator,
        oauthConfigSpecification.completeOauthOutputSpecification,
        completeOAuthFlow.keys,
      ) { resultMap, key -> resultMap.put(key, completeOAuthFlow[key]!!) }

    val oAuthServerOutputs: Map<String, Any> =
      formatOAuthOutput(
        validator,
        oauthConfigSpecification.completeOauthServerOutputSpecification,
        Jsons.keys(oauthParamConfig),
      ) { resultMap, key -> resultMap.put(key, MoreOAuthParameters.SECRET_MASK) }

    return MoreMaps.merge(oAuthServerOutputs, oAuthOutputs)
  }

  /**
   * Retrieves the OAuth declarative input specification from the provided OAuthConfigSpecification.
   * If the specification contains a "properties" field, it returns the value of that field.
   * Otherwise, it returns the entire OAuth declarative input specification. If the provided
   * OAuthConfigSpecification is null or does not contain an OAuth connector input specification, it
   * returns an empty JSON object.
   *
   * @param oauthConfigSpecification the OAuth configuration specification containing the input
   * specification
   * @return the OAuth declarative input specification or an empty JSON object if the input
   * specification is not available
   */
  protected fun getOAuthDeclarativeInputSpec(oauthConfigSpecification: OAuthConfigSpecification?): JsonNode {
    if (oauthConfigSpecification == null || oauthConfigSpecification.oauthConnectorInputSpecification == null) {
      return Jsons.emptyObject()
    }

    val oauthDeclarativeInputSpec = oauthConfigSpecification.oauthConnectorInputSpecification
    val propertiesNode = if (oauthDeclarativeInputSpec.has(PROPERTIES)) oauthDeclarativeInputSpec[PROPERTIES] else oauthDeclarativeInputSpec
    setExtractOutput(oauthConfigSpecification, propertiesNode)

    return propertiesNode
  }

  /**
   * Extracts a list of output paths from the complete OAuth output specification.
   *
   * @param oauthConfigSpecification the OAuth configuration specification containing the complete
   * OAuth output specification
   * @return a list of strings representing the paths in the complete OAuth output specification
   * @throws IllegalArgumentException if any 'path_in_complete_oauth' parameter is missing or empty in
   * the complete OAuth output specification
   */
  protected fun getExtractOutputFromCompleteOauthOutputSpec(oauthConfigSpecification: OAuthConfigSpecification): List<String> {
    val completeOauthOutputSpec = oauthConfigSpecification.completeOauthOutputSpecification[PROPERTIES]
    val extractOutput: MutableList<String> = ArrayList()

    completeOauthOutputSpec.fields().forEachRemaining { entry: Map.Entry<String?, JsonNode> ->
      val pathNode = entry.value[PATH_IN_OAUTH_RESPONSE]
      if (pathNode != null && pathNode.isArray && pathNode.size() > 0) {
        extractOutput.add(Jsons.stringListToJoinedString(pathNode, "."))
      } else {
        val msg = "The '%s' parameter for the '%s' in `complete_oauth_output_specification` is missing or empty."
        throw IllegalArgumentException(String.format(msg, PATH_IN_OAUTH_RESPONSE, entry.key))
      }
    }

    return extractOutput
  }

  /**
   * Sets the extract output in the OAuth declarative input specification if it does not already
   * exist.
   *
   * @param oauthConfigSpecification the OAuth configuration specification
   * @param oauthDeclarativeInputSpec the JSON node representing the OAuth declarative input
   * specification
   */
  protected fun setExtractOutput(
    oauthConfigSpecification: OAuthConfigSpecification,
    oauthDeclarativeInputSpec: JsonNode,
  ) {
    if (!oauthDeclarativeInputSpec.has(EXTRACT_OUTPUT_KEY)) {
      Jsons.setNode(oauthDeclarativeInputSpec, EXTRACT_OUTPUT_KEY, getExtractOutputFromCompleteOauthOutputSpec(oauthConfigSpecification))
    }
  }

  /**
   * This function should be redefined in each OAuthFlow implementation to isolate such "hardcoded"
   * values. It is being @deprecated because the output path should not be "hard-coded" in the OAuth
   * flow implementation classes anymore but will be specified as part of the OAuth Specification
   * object
   */
  @Deprecated("")
  abstract fun getDefaultOAuthOutputPath(): List<String>

  companion object {
    private const val PROPERTIES = "properties"
    private const val PATH_IN_OAUTH_RESPONSE = "path_in_oauth_response"
    private const val EXTRACT_OUTPUT_KEY = "extract_output"

    /**
     * Retrieves the value of the specified field from the given OAuth configuration JSON node. If the
     * field is not present in the JSON node, an IllegalArgumentException is thrown.
     *
     * @param oauthConfig the JSON node containing the OAuth configuration.
     * @param fieldName the name of the field to retrieve the value for.
     * @return the value of the specified field as a String.
     * @throws IllegalArgumentException if the specified field is not present in the JSON node.
     */
    @JvmStatic
    protected fun getConfigValueUnsafe(
      oauthConfig: JsonNode?,
      fieldName: String?,
    ): String {
      if (oauthConfig != null && oauthConfig[fieldName] != null) {
        return oauthConfig[fieldName].asText()
      } else {
        throw IllegalArgumentException(String.format("Undefined parameter '%s' necessary for the OAuth Flow.", fieldName))
      }
    }

    /**
     * Formats the OAuth output by validating and replacing specified keys in the output schema.
     *
     * @param validator the JsonSchemaValidator used to validate the output schema
     * @param outputSchema the JSON schema that defines the structure of the output
     * @param keys the collection of keys to be replaced in the output schema
     * @param replacement the BiConsumer that performs the replacement operation on the map builder for
     * each key
     * @return a map containing the formatted OAuth output
     * @throws JsonValidationException if the output schema validation fails
     */
    @Throws(JsonValidationException::class)
    private fun formatOAuthOutput(
      validator: JsonSchemaValidator,
      outputSchema: JsonNode?,
      keys: Collection<String>,
      replacement: BiConsumer<ImmutableMap.Builder<String, Any>, String>,
    ): Map<String, Any> {
      var result = java.util.Map.of<String, Any>()
      if (outputSchema != null && outputSchema.has(PROPERTIES)) {
        val mapBuilder = ImmutableMap.builder<String, Any>()
        for (key in keys) {
          if (outputSchema[PROPERTIES].has(key)) {
            replacement.accept(mapBuilder, key)
          }
        }
        result = mapBuilder.build()
        validator.ensure(outputSchema, Jsons.jsonNode<Map<String, Any>>(result))
      }
      return result
    }
  }
}
