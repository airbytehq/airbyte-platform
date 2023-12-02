/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.micronaut.core.util.StringUtils;
import jakarta.inject.Singleton;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class that facilitates the extraction of values from HTTP request POST bodies.
 */
@Singleton
@Slf4j
public class AirbyteHttpRequestFieldExtractor {

  // For some APIs we asked for a list of ids, such as workspace IDs and connection IDs. We will
  // validate if user has permission
  // to all of them.
  private static final Set<String> ARRAY_FIELDS =
      Set.of(AuthenticationFields.WORKSPACE_IDS_FIELD_NAME, AuthenticationFields.CONNECTION_IDS_FIELD_NAME);

  /**
   * Extracts the requested ID from the HTTP request, if present.
   *
   * @param content The raw HTTP request as a string.
   * @param idFieldName The name of the field/header that contains the ID.
   * @return An {@link Optional} that may or may not contain the ID value extracted from the raw HTTP
   *         request.
   */
  public Optional<String> extractId(final String content, final String idFieldName) {
    try {
      final JsonNode json = Jsons.deserialize(content);
      if (json != null) {

        final Optional<String> idValue = extract(json, idFieldName);

        if (idValue.isEmpty()) {
          log.debug("No match for field name '{}' in content '{}'.", idFieldName, content);
        } else {
          log.debug("Found '{}' for field '{}'", idValue, idFieldName);
          return idValue;
        }
      }
    } catch (final RuntimeException e) {
      log.debug("Unable to extract ID field '{}' from content '{}'.", idFieldName, content, e);
    }

    return Optional.empty();
  }

  private Optional<String> extract(JsonNode jsonNode, String idFieldName) {
    if (ARRAY_FIELDS.contains(idFieldName)) {
      log.debug("Try to extract list of ids for field {}", idFieldName);
      return Optional.ofNullable(jsonNode.get(idFieldName))
          .map(Jsons::serialize)
          .filter(StringUtils::hasText);
    } else {
      return Optional.ofNullable(jsonNode.get(idFieldName))
          .map(JsonNode::asText)
          .filter(StringUtils::hasText);
    }
  }

}
