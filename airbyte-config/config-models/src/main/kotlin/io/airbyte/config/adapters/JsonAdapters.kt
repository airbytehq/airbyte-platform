package io.airbyte.config.adapters

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.BooleanNode
import com.fasterxml.jackson.databind.node.DoubleNode
import com.fasterxml.jackson.databind.node.IntNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import io.airbyte.config.StreamDescriptor
import io.airbyte.protocol.models.AirbyteMessage

class JsonValueAdapter(private val node: JsonNode) : Value {
  override fun asBoolean(): Boolean = node.asBoolean()

  override fun asNumber(): Number = node.asDouble()

  override fun asString(): String = node.asText()
}

data class AirbyteJsonRecordAdapter(private val message: AirbyteMessage) : AirbyteRecord {
  override val asProtocol: AirbyteMessage
    get() = message
  override val streamDescriptor: StreamDescriptor = StreamDescriptor().withNamespace(message.record.namespace).withName(message.record.stream)
  private val data: ObjectNode = message.record.data as ObjectNode

  override fun has(fieldName: String): Boolean = data.has(fieldName)

  override fun get(fieldName: String): Value = JsonValueAdapter(data.get(fieldName))

  override fun remove(fieldName: String) {
    data.remove(fieldName)
  }

  override fun <T : Any> set(
    fieldName: String,
    value: T,
  ) {
    data.set<JsonNode>(fieldName, createNode(value))
  }

  private fun <T : Any> createNode(value: T): JsonNode =
    when (value) {
      is Boolean -> BooleanNode.valueOf(value)
      is Double -> DoubleNode.valueOf(value)
      is Int -> IntNode.valueOf(value)
      is String -> TextNode.valueOf(value)
      else -> TODO("Unsupported type ${value::class.java.name}")
    }
}
