/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.micronaut.core.util.StringUtils;
import jakarta.inject.Singleton;
import java.lang.invoke.MethodHandles;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class that facilitates the extraction of values from HTTP request POST bodies.
 */
@Singleton
public class AirbyteHttpRequestFieldExtractor {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // For some APIs we asked for a list of ids, such as workspace IDs and connection IDs. We will
  // validate if user has permission
  // to all of them.
  private static final Set<String> ARRAY_FIELDS =
      Set.of(AuthenticationFields.WORKSPACE_IDS_FIELD_NAME, AuthenticationFields.CONNECTION_IDS_FIELD_NAME);

  /**
   * Extracts the requested ID from the HTTP request, if present.
   *
   * @param json The HTTP request body as a JsonNode.
   * @param idFieldName The name of the field/header that contains the ID.
   * @return An {@link Optional} that may or may not contain the ID value extracted from the raw HTTP
   *         request.
   */
  public Optional<String> extractId(final JsonNode json, final String idFieldName) {
    try {
      if (json != null) {

        final Optional<String> idValue = extract(json, idFieldName);

        if (idValue.isEmpty()) {
          log.trace("No match for field name '{}' in content '{}'.", idFieldName, json);
        } else {
          log.trace("Found '{}' for field '{}'", idValue, idFieldName);
          return idValue;
        }
      }
    } catch (final RuntimeException e) {
      log.trace("Unable to extract ID field '{}' from content '{}'.", idFieldName, json, e);
    }

    return Optional.empty();
  }

  private Optional<String> extract(JsonNode jsonNode, String idFieldName) {
    if (ARRAY_FIELDS.contains(idFieldName)) {
      log.trace("Try to extract list of ids for field {}", idFieldName);
      return Optional.ofNullable(jsonNode.get(idFieldName))
          .map(Jsons::serialize)
          .filter(StringUtils::hasText);
    } else {
      return Optional.ofNullable(jsonNode.get(idFieldName))
          .map(JsonNode::asText)
          .filter(StringUtils::hasText);
    }
  }

  public Optional<JsonNode> contentToJson(final String contentAsString) {
    // Not sure if we'd ever have to worry about this case, but guarding against it anyway.
    if (contentAsString.isBlank()) {
      return Optional.empty();
    }
    try {
      final JsonNode contentAsJson = Jsons.deserialize(contentAsString);
      return Optional.of(contentAsJson);
    } catch (RuntimeException e) {
      log.error("Failed to parse content as JSON: {}", contentAsString, e);
      return Optional.empty();
    }
  }

}
