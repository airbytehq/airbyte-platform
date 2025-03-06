/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.adapters

import io.airbyte.commons.enums.Enums
import io.airbyte.commons.json.Jsons
import io.airbyte.config.StreamDescriptor
import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.protocol.models.AirbyteRecordMessage
import io.airbyte.protocol.models.AirbyteRecordMessageMeta
import io.airbyte.protocol.models.AirbyteRecordMessageMetaChange

class TestValueAdapter(
  private val value: Any,
) : Value {
  override fun asBoolean(): Boolean = value as Boolean

  override fun asNumber(): Number = value as Number

  override fun asString(): String = value.toString()
}

class TestRecordAdapter(
  override val streamDescriptor: StreamDescriptor,
  data: Map<String, Any>,
) : AirbyteRecord {
  private var shouldInclude = true

  data class Change(
    val fieldName: String,
    val change: AirbyteRecord.Change,
    val reason: AirbyteRecord.Reason,
  )

  private val data: MutableMap<String, Any> = data.toMutableMap()
  private val _changes: MutableList<Change> = mutableListOf()

  val changes: List<Change>
    get(): List<Change> = _changes.toList()

  override val asProtocol: AirbyteMessage
    get() =
      AirbyteMessage()
        .withRecord(
          AirbyteRecordMessage()
            .withStream(streamDescriptor.name)
            .withNamespace(streamDescriptor.namespace)
            .withData(Jsons.jsonNode(data))
            .withMeta(
              AirbyteRecordMessageMeta().withChanges(
                _changes.map {
                  AirbyteRecordMessageMetaChange()
                    .withChange(Enums.convertTo(it.change, AirbyteRecordMessageMetaChange.Change::class.java))
                    .withField(it.fieldName)
                    .withReason(Enums.convertTo(it.reason, AirbyteRecordMessageMetaChange.Reason::class.java))
                },
              ),
            ),
        )

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
