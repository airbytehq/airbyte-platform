/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.adapters

import io.airbyte.config.StreamDescriptor
import io.airbyte.mappers.adapters.AirbyteRecord
import io.airbyte.mappers.adapters.Value

class TestValueAdapter(
  private val value: Any,
) : Value {
  override fun asBoolean(): Boolean = value as Boolean

  override fun asNumber(): Number = value as Number

  override fun asString(): String = value.toString()
}

class TestRecordAdapter(
  val streamDescriptor: StreamDescriptor,
  val data: MutableMap<String, Any>,
) : AirbyteRecord {
  private var shouldInclude = true

  data class Change(
    val fieldName: String,
    val change: AirbyteRecord.Change,
    val reason: AirbyteRecord.Reason,
  )

  private val _changes: MutableList<Change> = mutableListOf()

  val changes: List<Change>
    get(): List<Change> = _changes.toList()

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

  override fun setInclude(value: Boolean) {
    shouldInclude = value
  }

  override fun shouldInclude(): Boolean = shouldInclude
}
