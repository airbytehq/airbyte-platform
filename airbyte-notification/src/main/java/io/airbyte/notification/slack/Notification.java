/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification.slack;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class Notification {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    MAPPER.setDateFormat(dateFormat);
    MAPPER.registerModule(new JavaTimeModule());
  }

  private String text;
  private final List<Block> blocks;

  private Object data;

  public Notification() {
    blocks = new ArrayList<>();
    data = null;
  }

  public void setText(String text) {
    this.text = text;
  }

  public void setData(Object data) {
    this.data = data;
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
    if (data != null) {
      node.put("data", MAPPER.valueToTree(data));
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
