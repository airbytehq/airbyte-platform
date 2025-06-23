/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification.slack

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import java.text.DateFormat
import java.text.SimpleDateFormat

class Notification {
  private var text: String? = null
  private val blocks: MutableList<Block> = ArrayList()

  private var data: Any? = null

  fun setText(text: String?) {
    this.text = text
  }

  fun setData(data: Any?) {
    this.data = data
  }

  fun addSection(): Section {
    val block = Section()
    blocks.add(block)
    return block
  }

  fun addDivider() {
    blocks.add(Divider())
  }

  fun toJsonNode(): JsonNode {
    val jsonNodeFactory = JsonNodeFactory.instance
    val node = jsonNodeFactory.objectNode()
    if (text != null) {
      node.put("text", text)
    }

    if (!blocks.isEmpty()) {
      val blocksNode = jsonNodeFactory.arrayNode()
      for (block in blocks) {
        blocksNode.add(block.toJsonNode())
      }
      node.put("blocks", blocksNode)
    }
    if (data != null) {
      node.put("data", MAPPER.valueToTree<JsonNode>(data))
    }
    return node
  }

  companion object {
    private val MAPPER = ObjectMapper()

    init {
      val dateFormat: DateFormat = SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
      MAPPER.setDateFormat(dateFormat)
      MAPPER.registerModule(JavaTimeModule())
    }

    fun createLink(
      text: String,
      url: String?,
    ): String {
      val escapedSequence =
        text
          .replace("&", "\$amp;")
          .replace(">", "&gt;")
          .replace("<", "&lt;")
      return String.format("<%s|%s>", url, escapedSequence)
    }
  }
}
