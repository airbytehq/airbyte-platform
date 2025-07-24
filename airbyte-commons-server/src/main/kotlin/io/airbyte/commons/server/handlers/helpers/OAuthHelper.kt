/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.BooleanNode
import com.fasterxml.jackson.databind.node.TextNode
import io.airbyte.api.model.generated.CompleteOAuthResponse
import io.airbyte.commons.json.Jsons
import io.airbyte.protocol.models.v0.OAuthConfigSpecification

/**
 * Static helpers for Oauth-related reading and writing.
 */
object OAuthHelper {
  private const val PROPERTIES = "properties"
  private const val PATH_IN_CONNECTOR_CONFIG = "path_in_connector_config"

  /**
   * Extract fields names from oauth spec.
   *
   * @param oauthSpec oauth spec
   * @return field names in the spec
   */
  @JvmStatic
  fun extractOauthConfigurationPaths(oauthSpec: JsonNode?): Map<String, List<String>> {
    if (oauthSpec != null && oauthSpec.has(PROPERTIES) && oauthSpec[PROPERTIES].isObject) {
      val result: MutableMap<String, List<String>> = HashMap()

      oauthSpec[PROPERTIES].fields().forEachRemaining { entry: Map.Entry<String, JsonNode> ->
        val value = entry.value
        if (value.isObject && value.has(PATH_IN_CONNECTOR_CONFIG) && value[PATH_IN_CONNECTOR_CONFIG].isArray) {
          val path: MutableList<String> = ArrayList()
          for (pathPart in value[PATH_IN_CONNECTOR_CONFIG]) {
            path.add(pathPart.textValue())
          }
          result[entry.key] = path
        }
      }

      return result
    } else {
      return HashMap()
    }
  }

  /**
   * Map to the result of a completeOauth request to an API response.
   *
   * @param input input
   * @return complete oauth response
   */
  @JvmStatic
  fun mapToCompleteOAuthResponse(input: Map<String, Any>): CompleteOAuthResponse {
    val response = CompleteOAuthResponse()
    response.authPayload = HashMap()

    if (input.containsKey("request_succeeded")) {
      response.requestSucceeded = "true" == input["request_succeeded"]
    } else {
      response.requestSucceeded = true
    }

    if (input.containsKey("request_error")) {
      response.requestError = input["request_error"].toString()
    }

    input.forEach { (k: String?, v: Any?) ->
      if ("request_succeeded" != k && "request_error" != k) {
        response.authPayload[k] = v
      }
    }

    return response
  }

  /**
   * Update the oauthUserInputFromConnectorConfigSpecification to allow for additional properties. The
   * testing values must define the required values, but can send along additional fields from the
   * testing values as well. TODO: Protocolize that this must always be set to true?
   */
  @JvmStatic
  fun updateOauthConfigToAcceptAdditionalUserInputProperties(oauthConfigSpecification: OAuthConfigSpecification) {
    val userInputNode = oauthConfigSpecification.oauthUserInputFromConnectorConfigSpecification
    val updatedNode = Jsons.getNodeOrEmptyObject(userInputNode)

    Jsons.setNestedValue(updatedNode, listOf("type"), TextNode.valueOf("object"))
    Jsons.setNestedValue(updatedNode, listOf("additionalProperties"), BooleanNode.TRUE)

    oauthConfigSpecification.oauthUserInputFromConnectorConfigSpecification = updatedNode
  }
}
