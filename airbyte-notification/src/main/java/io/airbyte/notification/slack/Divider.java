/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification.slack;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Divider implements Block {

  @Override
  public JsonNode toJsonNode() {
    JsonNodeFactory jsonNodeFactory = JsonNodeFactory.instance;
    ObjectNode node = jsonNodeFactory.objectNode();
    node.put("type", "divider");
    return node;
  }

}
