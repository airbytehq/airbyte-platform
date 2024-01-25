/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification.slack;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;

public class Notification {

  private String text;
  private final List<Block> blocks;

  public Notification() {
    blocks = new ArrayList<>();
  }

  public void setText(String text) {
    this.text = text;
  }

  public Section addSection() {
    Section block = new Section();
    this.blocks.add(block);
    return block;
  }

  public void addDivider() {
    this.blocks.add(new Divider());
  }

  public JsonNode toJsonNode() {
    JsonNodeFactory jsonNodeFactory = JsonNodeFactory.instance;
    ObjectNode node = jsonNodeFactory.objectNode();
    if (text != null) {
      node.put("text", text);
    }

    if (!blocks.isEmpty()) {
      ArrayNode blocksNode = jsonNodeFactory.arrayNode();
      for (Block block : blocks) {
        blocksNode.add(block.toJsonNode());
      }
      node.put("blocks", blocksNode);
    }
    return node;
  }

  public static String createLink(String text, String url) {
    String escapedSequence = text.replace("&", "$amp;")
        .replace(">", "&gt;")
        .replace("<", "&lt;");
    return String.format("<%s|%s>", url, escapedSequence);
  }

}
