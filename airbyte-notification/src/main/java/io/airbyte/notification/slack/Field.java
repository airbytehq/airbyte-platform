/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification.slack;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Field {

  private String type;
  private String text;

  public void setType(String type) {
    this.type = type;
  }

  public void setText(String text) {
    this.text = text;
  }

  public JsonNode toJsonNode() {
    JsonNodeFactory jsonNodeFactory = JsonNodeFactory.instance;
    ObjectNode node = jsonNodeFactory.objectNode();
    node.put("type", type);
    node.put("text", text);
    return node;
  }

}
