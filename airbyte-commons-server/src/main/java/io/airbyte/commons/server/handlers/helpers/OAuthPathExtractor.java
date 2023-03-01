/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Extract paths to oauth fields from an oauth spec.
 */
public class OAuthPathExtractor {

  private static final String PROPERTIES = "properties";
  private static final String PATH_IN_CONNECTOR_CONFIG = "path_in_connector_config";

  /**
   * Extract fields names from oauth spec.
   *
   * @param oauthSpec oauth spec
   * @return field names in the spec
   */
  public static Map<String, List<String>> extractOauthConfigurationPaths(final JsonNode oauthSpec) {

    if (oauthSpec != null && oauthSpec.has(PROPERTIES) && oauthSpec.get(PROPERTIES).isObject()) {
      final Map<String, List<String>> result = new HashMap<>();

      oauthSpec.get(PROPERTIES).fields().forEachRemaining(entry -> {
        final JsonNode value = entry.getValue();
        if (value.isObject() && value.has(PATH_IN_CONNECTOR_CONFIG) && value.get(PATH_IN_CONNECTOR_CONFIG).isArray()) {
          final List<String> path = new ArrayList<>();
          for (final JsonNode pathPart : value.get(PATH_IN_CONNECTOR_CONFIG)) {
            path.add(pathPart.textValue());
          }
          result.put(entry.getKey(), path);
        }
      });

      return result;
    } else {
      return new HashMap<>();
    }
  }

}
