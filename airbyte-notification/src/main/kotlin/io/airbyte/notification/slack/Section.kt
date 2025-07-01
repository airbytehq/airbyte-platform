/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification.slack

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory

class Section : Block {
  private var text: String? = null

  private val fields: MutableList<Field> =
    ArrayList()

  fun addField(): Field {
    val field = Field()
    fields.add(field)
    return field
  }

  fun setText(text: String?) {
    this.text = text
  }

  override fun toJsonNode(): JsonNode {
    val jsonNodeFactory = JsonNodeFactory.instance
    val node = jsonNodeFactory.objectNode()
    node.put("type", "section")

    if (text != null) {
      val textNode = jsonNodeFactory.objectNode()
      textNode.put("type", "mrkdwn")
      textNode.put("text", text)
      node.put("text", textNode)
    }
    if (!fields.isEmpty()) {
      val fieldsNode = jsonNodeFactory.arrayNode()
      for (field in fields) {
        fieldsNode.add(field.toJsonNode())
      }
      node.put("fields", fieldsNode)
    }
    return node
  }
}
