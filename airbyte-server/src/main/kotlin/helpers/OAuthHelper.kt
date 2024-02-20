/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.helpers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.server.problems.InvalidRedirectUrlProblem
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.URI

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
