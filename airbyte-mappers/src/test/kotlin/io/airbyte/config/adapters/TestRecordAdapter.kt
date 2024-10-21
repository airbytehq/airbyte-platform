package io.airbyte.config.adapters

import io.airbyte.config.StreamDescriptor
import io.airbyte.protocol.models.AirbyteMessage

class TestValueAdapter(private val value: Any) : Value {
  override fun asBoolean(): Boolean = value as Boolean

  override fun asNumber(): Number = value as Number

  override fun asString(): String = value.toString()
}

class TestRecordAdapter(override val streamDescriptor: StreamDescriptor, data: Map<String, Any>) : AirbyteRecord {
  data class Change(val fieldName: String, val change: AirbyteRecord.Change, val reason: AirbyteRecord.Reason)

  private val data: MutableMap<String, Any> = data.toMutableMap()
  private val _changes: MutableList<Change> = mutableListOf()

  val changes: List<Change>
    get(): List<Change> = _changes.toList()

  override val asProtocol: AirbyteMessage
    get() = TODO("Not yet implemented")

  override fun has(fieldName: String): Boolean = fieldName in data

  override fun get(fieldName: String): Value = TestValueAdapter(data[fieldName] as Any)

  override fun remove(fieldName: String) {
    data.remove(fieldName)
  }

  override fun rename(
    oldFieldName: String,
    newFieldName: String,
  ) {
    data[newFieldName] = data[oldFieldName] as Any
    data.remove(oldFieldName)
  }

  override fun <T : Any> set(
    fieldName: String,
    value: T,
  ) {
    data[fieldName] = value
  }

  override fun trackFieldError(
    fieldName: String,
    change: AirbyteRecord.Change,
    reason: AirbyteRecord.Reason,
  ) {
    _changes.add(Change(fieldName, change, reason))
  }
}
