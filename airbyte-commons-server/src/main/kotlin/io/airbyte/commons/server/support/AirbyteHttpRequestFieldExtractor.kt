/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.core.util.StringUtils
import jakarta.inject.Singleton
import java.util.Optional

/**
 * Utility class that facilitates the extraction of values from HTTP request POST bodies.
 */
@Singleton
class AirbyteHttpRequestFieldExtractor {
  /**
   * Extracts the requested ID from the HTTP request, if present.
   *
   * @param json The HTTP request body as a JsonNode.
   * @param idFieldName The name of the field/header that contains the ID.
   * @return An [Optional] that may or may not contain the ID value extracted from the raw HTTP
   * request.
   */
  fun extractId(
    json: JsonNode?,
    idFieldName: String,
  ): Optional<String> {
    try {
      if (json != null) {
        val idValue = extract(json, idFieldName)

        if (idValue.isEmpty) {
          log.trace("No match for field name '{}' in content '{}'.", idFieldName, json)
        } else {
          log.trace("Found '{}' for field '{}'", idValue, idFieldName)
          return idValue
        }
      }
    } catch (e: RuntimeException) {
      log.trace("Unable to extract ID field '{}' from content '{}'.", idFieldName, json, e)
    }

    return Optional.empty()
  }

  private fun extract(
    jsonNode: JsonNode,
    idFieldName: String,
  ): Optional<String> {
    if (ARRAY_FIELDS.contains(idFieldName)) {
      log.trace("Try to extract list of ids for field {}", idFieldName)
      return Optional
        .ofNullable(jsonNode[idFieldName])
        .map { `object`: JsonNode? -> Jsons.serialize(`object`) }
        .filter { str: String? -> StringUtils.hasText(str) }
    } else {
      return Optional
        .ofNullable(jsonNode[idFieldName])
        .map { obj: JsonNode -> obj.asText() }
        .filter { str: String? -> StringUtils.hasText(str) }
    }
  }

  fun contentToJson(contentAsString: String): Optional<JsonNode> {
    // Not sure if we'd ever have to worry about this case, but guarding against it anyway.
    if (contentAsString.isBlank()) {
      return Optional.empty()
    }
    try {
      val contentAsJson = Jsons.deserialize(contentAsString)
      return Optional.of(contentAsJson)
    } catch (e: RuntimeException) {
      log.error("Failed to parse content as JSON: {}", contentAsString, e)
      return Optional.empty()
    }
  }

  companion object {
    private val log = KotlinLogging.logger {}

    // For some APIs we asked for a list of ids, such as workspace IDs and connection IDs. We will
    // validate if user has permission
    // to all of them.
    private val ARRAY_FIELDS: Set<String> =
      setOf(AuthenticationFields.WORKSPACE_IDS_FIELD_NAME, AuthenticationFields.CONNECTION_IDS_FIELD_NAME)
  }
}
