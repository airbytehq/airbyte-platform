/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.errors.BadObjectSchemaKnownException
import io.airbyte.commons.server.handlers.helpers.OAuthHelper.extractOauthConfigurationPaths
import io.airbyte.oauth.MoreOAuthParameters.flattenOAuthConfig
import io.airbyte.persistence.job.factory.OAuthConfigSupplier.Companion.hasOAuthConfigSpecification
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.validation.json.JsonValidationException

/**
 * Holds helpers to handle OAuth secrets.
 */
object OAuthSecretHelper {
  /**
   * Should mimic frontend in using connector specification to figure out what auth properties are
   * necessary for source creation and where to put them.
   */
  @JvmStatic
  @Throws(JsonValidationException::class)
  fun setSecretsInConnectionConfiguration(
    spec: ConnectorSpecification,
    hydratedSecret: JsonNode,
    connectionConfiguration: JsonNode,
  ): JsonNode {
    // Get the paths from advancedAuth that we need

    val newConnectionConfiguration = connectionConfiguration.deepCopy<ObjectNode>()
    val oauthPaths = getOAuthConfigPaths(spec)
    val flattenedSecret = flattenOAuthConfig(hydratedSecret)

    for ((key, jsonPathList) in oauthPaths) {
      // Key where we need to stuff things

      Jsons.setNestedValue(newConnectionConfiguration, jsonPathList, flattenedSecret[key])
    }
    return newConnectionConfiguration
  }

  /**
   * From advanced_auth gets the data we need to put into the connectionConfiguration which we get
   * from both complete_oauth and the actor_oauth_params Returns a map of the property_name:
   * path_in_connector_config i.e. { client_id: ['credentials', 'client_id']}
   */
  @VisibleForTesting
  @Throws(JsonValidationException::class)
  fun getAdvancedAuthOAuthPaths(
    connectorSpecification: ConnectorSpecification,
    includeOutputPaths: Boolean,
  ): Map<String, List<String>> {
    if (hasOAuthConfigSpecification(connectorSpecification)) {
      val completeOAuthOutputSpecification =
        connectorSpecification.advancedAuth.oauthConfigSpecification.completeOauthOutputSpecification
      val completeOAuthServerOutputSpecification =
        connectorSpecification.advancedAuth.oauthConfigSpecification.completeOauthServerOutputSpecification

      // Merge all the mappings into one map
      val result: MutableMap<String, List<String>> = HashMap(extractOauthConfigurationPaths(completeOAuthServerOutputSpecification))
      if (includeOutputPaths) {
        result.putAll(extractOauthConfigurationPaths(completeOAuthOutputSpecification))
      }
      return result
    } else {
      throw JsonValidationException(
        String.format("Error parsing advancedAuth - see [%s]", connectorSpecification.documentationUrl),
      )
    }
  }

  /**
   * Standardizes out the return format for getting config paths whether it's a legacy OAuth spec or
   * an advanced_auth one. Returns all output paths, used for setting secrets.
   *
   * @param spec - connector specification to get paths for
   * @return Map where the key = the property and the value = the path to the property in list form.
   */
  @JvmStatic
  @Throws(JsonValidationException::class)
  fun getOAuthConfigPaths(spec: ConnectorSpecification): Map<String, List<String>> {
    if (hasOAuthConfigSpecification(spec)) {
      return getAdvancedAuthOAuthPaths(spec, true)
    } else {
      throw IllegalStateException("No OAuth data in specification")
    }
  }

  /**
   * Like getOAuthConfigPaths but does not include the server output paths in case users need to
   * change them independently. Used for validation.
   *
   * @param spec - connector specification to get paths for
   * @return Map where the key = the property and the value = the path to the property in list form.
   */
  @JvmStatic
  @Throws(JsonValidationException::class)
  fun getOAuthInputPaths(spec: ConnectorSpecification): Map<String, List<String>> {
    if (hasOAuthConfigSpecification(spec)) {
      return getAdvancedAuthOAuthPaths(spec, false)
    } else {
      throw IllegalStateException("No OAuth data in specification")
    }
  }

  /**
   * Get OAuth secret paths but only for the completeOauthServerOutput portion of the connector
   * specification.
   *
   * @param connectorSpecification connector specification from source/destination version
   * @return Map of property: path in connector config.
   * @throws JsonValidationException if we don't have an oauth config specification to parse.
   */
  @Throws(JsonValidationException::class)
  fun getCompleteOauthServerOutputPaths(connectorSpecification: ConnectorSpecification): Map<String, List<String>> {
    if (hasOAuthConfigSpecification(connectorSpecification)) {
      val completeOAuthServerOutputSpecification =
        connectorSpecification.advancedAuth.oauthConfigSpecification.completeOauthServerOutputSpecification

      // Merge all the mappings into one map
      return HashMap(extractOauthConfigurationPaths(completeOAuthServerOutputSpecification))
    } else {
      throw JsonValidationException(
        String.format("Error parsing advancedAuth - see [%s]", connectorSpecification.documentationUrl),
      )
    }
  }

  /**
   * Traverses advancedAuth structure to validate the input oauth param configuration. As it
   * traverses, it builds up a connector specification that we can pass into statefulSplitSecrets to
   * get back an oauth param configuration that has had its secrets stripped out and replaced
   * appropriately. I could have split these into separate functions, but they would have just done
   * the same loop. For advanced auth sources/destinations, we don't actually mark the secrets in the
   * connector configuration For this to work with statefulSplitSecrets we need to spoof the
   * connection configuration so that the oauth params are "secrets" and so that the connection
   * configuration can be fed into statefulSplitSecrets and return us a properly sanitized oauth param
   * config.
   *
   * @param connectorSpecification connector specification
   * @param oauthParamConfiguration the passed in oauth param configuration
   * @return a connector specification that has each path from CompleteOauthServerOutputPaths set to
   * have airbyte_secret: true
   * @throws JsonValidationException If there is no valid OauthConfig Specification.
   */
  @JvmStatic
  @VisibleForTesting
  @Throws(JsonValidationException::class)
  fun validateOauthParamConfigAndReturnAdvancedAuthSecretSpec(
    connectorSpecification: ConnectorSpecification,
    oauthParamConfiguration: JsonNode?,
  ): ConnectorSpecification {
    if (hasOAuthConfigSpecification(connectorSpecification)) {
      val newConnectorSpecificationNode = Jsons.emptyObject()
      val airbyteSecret = java.util.Map.of("airbyte_secret", true)
      val oauthPaths = getCompleteOauthServerOutputPaths(connectorSpecification)
      for ((_, jsonPathList) in oauthPaths) {
        if (Jsons.navigateTo(oauthParamConfiguration, jsonPathList) == null) {
          throw BadObjectSchemaKnownException(String.format("Missing OAuth param for key at %s", jsonPathList))
        }
        Jsons.setNestedValue(newConnectorSpecificationNode, alternatingList("properties", jsonPathList), Jsons.jsonNode(airbyteSecret))
      }

      return ConnectorSpecification().withConnectionSpecification(newConnectorSpecificationNode)
    }

    throw BadObjectSchemaKnownException("No valid OAuth config specification")
  }

  /**
   * Create a list with alternating elements of property, list[n]. Used to spoof a connector
   * specification for splitting out secrets.
   *
   * @param property property to put in front of each list element
   * @param list list to insert elements into
   * @return new list with alternating elements
   */
  private fun alternatingList(
    property: String,
    list: List<String>,
  ): List<String> {
    val result: MutableList<String> = ArrayList(list.size * 2)

    for (item in list) {
      result.add(property)
      result.add(item)
    }
    return result
  }

  /**
   * Throws an exception if any property is set in the given configuration which corresponds to an
   * airbyte_secret field which might be injected by the server in the spec.
   */
  @JvmStatic
  @Throws(JsonValidationException::class)
  fun validateNoSecretsInConfiguration(
    spec: ConnectorSpecification,
    connectionConfiguration: JsonNode?,
  ) {
    if (hasOAuthConfigSpecification(spec)) {
      val oauthPaths = getOAuthInputPaths(spec)
      for ((key, jsonPathList) in oauthPaths) {
        throwIfKeyExistsInConfig(connectionConfiguration, key, jsonPathList)
      }
    }
  }

  private fun throwIfKeyExistsInConfig(
    connectionConfiguration: JsonNode?,
    key: String,
    jsonPathList: List<String>,
  ) {
    if (Jsons.navigateTo(connectionConfiguration, jsonPathList) != null) {
      // The API referenced by this message is a Cloud feature and not yet available in the open source
      // project but will be added.
      val errorMessage =
        String.format(
          "Cannot set key '%s', please create an OAuth credentials override instead - https://reference.airbyte.com/reference/workspaceoauthcredentials",
          key,
        )
      throw BadObjectSchemaKnownException(errorMessage)
    }
  }
}
