/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification.slack;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;

public class Section implements Block {

  private String text;

  private final List<Field> fields;

  public Section() {
    fields = new ArrayList<>();
  }

  public Field addField() {
    Field field = new Field();
    fields.add(field);
    return field;
  }

  public void setText(String text) {
    this.text = text;
  }

  @Override
  public JsonNode toJsonNode() {
    JsonNodeFactory jsonNodeFactory = JsonNodeFactory.instance;
    ObjectNode node = jsonNodeFactory.objectNode();
    node.put("type", "section");

    if (text != null) {
      ObjectNode textNode = jsonNodeFactory.objectNode();
      textNode.put("type", "mrkdwn");
      textNode.put("text", text);
      node.put("text", textNode);
    }
    if (!fields.isEmpty()) {
      ArrayNode fieldsNode = jsonNodeFactory.arrayNode();
      for (Field field : fields) {
        fieldsNode.add(field.toJsonNode());
      }
      node.put("fields", fieldsNode);
    }
    return node;
  }

}
