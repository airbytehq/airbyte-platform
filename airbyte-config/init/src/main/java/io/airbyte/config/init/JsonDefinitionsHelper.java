/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Helpers for updating json definitions.
 */
public class JsonDefinitionsHelper {

  /**
   * Add missing tombstone field and set it to false if it does not exist.
   *
   * @param definitionJson JSON Schema
   * @return schema with tombstone field added if needed
   */
  public static JsonNode addMissingTombstoneField(final JsonNode definitionJson) {
    final JsonNode currTombstone = definitionJson.get("tombstone");
    if (currTombstone == null || currTombstone.isNull()) {
      ((ObjectNode) definitionJson).set("tombstone", BooleanNode.FALSE);
    }
    return definitionJson;
  }

  /**
   * Add missing public field and set it to false if it does not exist.
   *
   * @param definitionJson JSON Schema
   * @return schema with public field added if needed
   */
  public static JsonNode addMissingPublicField(final JsonNode definitionJson) {
    final JsonNode currPublic = definitionJson.get("public");
    if (currPublic == null || currPublic.isNull()) {
      // definitions loaded from seed yamls are by definition public
      ((ObjectNode) definitionJson).set("public", BooleanNode.TRUE);
    }
    return definitionJson;
  }

  /**
   * Add missing custom field and set it to false if it does not exist.
   *
   * @param definitionJson JSON Schema
   * @return schema with custom field added if needed
   */
  public static JsonNode addMissingCustomField(final JsonNode definitionJson) {
    final JsonNode currCustom = definitionJson.get("custom");
    if (currCustom == null || currCustom.isNull()) {
      // definitions loaded from seed yamls are by definition not custom
      ((ObjectNode) definitionJson).set("custom", BooleanNode.FALSE);
    }
    return definitionJson;
  }

}
