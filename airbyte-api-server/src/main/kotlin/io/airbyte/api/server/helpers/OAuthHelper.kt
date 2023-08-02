/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.helpers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.server.problems.InvalidRedirectUrlProblem
import io.airbyte.commons.json.Jsons
import io.airbyte.protocol.models.ConnectorSpecification
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.URI
import java.util.stream.Stream

/**
 * Helper for OAuth.
 */
object OAuthHelper {
  private const val TEMP_OAUTH_STATE_KEY = "temp_oauth_state"
  private const val HTTPS = "https"
  private val OBJECT_MAPPER = ObjectMapper()
  private const val PROPERTIES = "properties"
  private val log = LoggerFactory.getLogger(OAuthHelper.javaClass)

  fun buildTempOAuthStateKey(state: String): String {
    return "$TEMP_OAUTH_STATE_KEY.$state"
  }

  /**
   * Helper function to validate that a redirect URL is valid and if not, return the appropriate
   * problem.
   */
  fun validateRedirectUrl(redirectUrl: String?) {
    if (redirectUrl == null) {
      throw InvalidRedirectUrlProblem("Redirect URL cannot be null")
    }
    try {
      val uri = URI.create(redirectUrl)
      if (uri.scheme != HTTPS) {
        throw InvalidRedirectUrlProblem("Redirect URL must use HTTPS")
      }
    } catch (e: IllegalArgumentException) {
      log.error(e.message)
      throw InvalidRedirectUrlProblem("Redirect URL must conform to RFC 2396 - https://www.ietf.org/rfc/rfc2396.txt")
    }
  }

  /**
   * Test if a connector spec has oauth configuration. copied from OAuthConfigSUpplier
   *
   * @param spec to check
   * @return true if it has an oauth config. otherwise, false.
   */
  fun hasOAuthConfigSpecification(spec: ConnectorSpecification?): Boolean {
    return spec != null && spec.advancedAuth != null && spec.advancedAuth.oauthConfigSpecification != null
  }

  /**
   * Test if a connector spec has legacy oauth configuration. copied from OAuthConfigSUpplier
   *
   * @param spec to check
   * @return true if it has a legacy oauth config. otherwise, false.
   */
  fun hasLegacyOAuthConfigSpecification(spec: ConnectorSpecification?): Boolean {
    return spec != null && spec.authSpecification != null && spec.authSpecification.oauth2Specification != null
  }

  /**
   * Extract oauth config specification.
   *
   * @param specification - connector specification
   * @return JsonNode of newly built OAuth spec.
   */
  fun extractOAuthConfigSpecification(specification: ConnectorSpecification): JsonNode {
    val oauthConfig: io.airbyte.protocol.models.OAuthConfigSpecification? = specification.advancedAuth.oauthConfigSpecification
    val completeOAuthServerOutputSpecification: JsonNode = oauthConfig?.completeOauthServerOutputSpecification!!
    val completeOAuthOutputSpecification: JsonNode = oauthConfig.completeOauthOutputSpecification!!
    val constructedSpecNode: JsonNode = Jsons.emptyObject()
    val oauthOutputPaths = extractFromCompleteOutputSpecification(completeOAuthOutputSpecification)
    val oauthServerOutputPaths = extractFromCompleteOutputSpecification(completeOAuthServerOutputSpecification)
    val required = Stream.concat(oauthOutputPaths.stream(), oauthServerOutputPaths.stream()).toList()

    // Try to get the original connector specification nodes so we retain documentation
    for (paramPath in required) {
      // Set required nodes
      val paramName = paramPath[paramPath.size - 1]
      var specNode: JsonNode = specification.connectionSpecification.get(PROPERTIES).findValue(paramName)

      // If we don't have one, resort to creating a naked node
      if (specNode == null) {
        val copyNode: Map<*, *> = java.util.Map.of("type", "string", "name", paramName)
        specNode = Jsons.jsonNode(copyNode)
      }
      Jsons.setNestedValue(constructedSpecNode, alternatingList(PROPERTIES, paramPath), specNode)
    }

    // Set title etc.
    Jsons.setNestedValue(constructedSpecNode, listOf("title"), specification.connectionSpecification.get("title"))
    return constructedSpecNode
  }

  private fun extractFromCompleteOutputSpecification(outputSpecification: JsonNode): List<List<String>> {
    val properties = outputSpecification[PROPERTIES]
    val paths = properties.findValues("path_in_connector_config")
    return paths.stream().map<List<String>> { node: JsonNode? ->
      try {
        return@map OBJECT_MAPPER.readerForListOf(String::class.java).readValue<Any>(node) as List<String>
      } catch (e: IOException) {
        throw RuntimeException(e)
      }
    }.toList()
  }

  /**
   * Extract legacy oauth specification.
   *
   * @param specification - connector specification
   * @return JsonNode of newly built OAuth spec.
   */
  fun extractLegacyOAuthSpecification(specification: ConnectorSpecification): JsonNode {
    val oauth2Specification: io.airbyte.protocol.models.OAuth2Specification? = specification.authSpecification.oauth2Specification
    val rootNode: List<Any> = oauth2Specification?.rootObject!!
    val specNodePath: List<String>
    val originalSpecNode: JsonNode
    val ONE = 1
    if (rootNode.size > ONE) {
      // Nested oneOf
      // specNode = the index of array node specified in the spec
      specNodePath = listOf(PROPERTIES, rootNode[0].toString())
      originalSpecNode = Jsons.navigateTo(specification.connectionSpecification, specNodePath).get("oneOf")
        .get(rootNode[1].toString().toInt())
    } else if (rootNode.size == ONE) {
      // just nested
      specNodePath = listOf(PROPERTIES, rootNode[0].toString())
      originalSpecNode = Jsons.navigateTo(specification.connectionSpecification, specNodePath)
    } else {
      // unnested
      specNodePath = listOf(PROPERTIES)
      originalSpecNode = specification.connectionSpecification
    }
    val fields = originalSpecNode[PROPERTIES].fields()
    val flatInitParams: MutableList<Any?>? = oauth2Specification.oauthFlowInitParameters.stream()
      .flatMap { obj: Collection<*> -> obj.stream() }.toList()

    // Remove fields that aren't in the init parameters
    while (fields.hasNext()) {
      val (key) = fields.next()
      if (!flatInitParams!!.contains(key)) {
        fields.remove()
      }
    }

    // create fields that aren't already in the original spec node
    // In some cases (trello, instagram etc.) we don't need the oauth params in the connector, but we do
    // need them to initiate oauth so they aren't
    // in the spec, but we still need them
    val requiredFields = OBJECT_MAPPER.valueToTree<ArrayNode>(flatInitParams)

    // Properly set required fields
    (originalSpecNode as ObjectNode).putArray("required").addAll(requiredFields)
    val constructedSpecNode: JsonNode = Jsons.emptyObject()
    Jsons.setNestedValue(constructedSpecNode, specNodePath, originalSpecNode)
    return constructedSpecNode
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
}
