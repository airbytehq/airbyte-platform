/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification.slack

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory

class Field {
  private var type: String? = null
  private var text: String? = null

  fun setType(type: String?) {
    this.type = type
  }

  fun setText(text: String?) {
    this.text = text
  }

  fun toJsonNode(): JsonNode {
    val jsonNodeFactory = JsonNodeFactory.instance
    val node = jsonNodeFactory.objectNode()
    node.put("type", type)
    node.put("text", text)
    return node
  }
}
