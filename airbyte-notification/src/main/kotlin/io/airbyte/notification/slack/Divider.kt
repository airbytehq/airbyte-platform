/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification.slack

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory

class Divider : Block {
  override fun toJsonNode(): JsonNode {
    val jsonNodeFactory = JsonNodeFactory.instance
    val node = jsonNodeFactory.objectNode()
    node.put("type", "divider")
    return node
  }
}
