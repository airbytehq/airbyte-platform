package io.airbyte.config.adapters

import io.airbyte.config.StreamDescriptor
import io.airbyte.protocol.models.AirbyteMessage

class TestValueAdapter(private val value: Any) : Value {
  override fun asBoolean(): Boolean = value as Boolean

  override fun asNumber(): Number = value as Number

  override fun asString(): String = value.toString()
}

class TestRecordAdapter(override val streamDescriptor: StreamDescriptor, data: Map<String, Any>) : AirbyteRecord {
  private val data: MutableMap<String, Any> = data.toMutableMap()

  override val asProtocol: AirbyteMessage
    get() = TODO("Not yet implemented")

  override fun has(fieldName: String): Boolean = fieldName in data

  override fun get(fieldName: String): Value = TestValueAdapter(data[fieldName] as Any)

  override fun remove(fieldName: String) {
    data.remove(fieldName)
  }

  override fun <T : Any> set(
    fieldName: String,
    value: T,
  ) {
    data[fieldName] = value
  }
}
