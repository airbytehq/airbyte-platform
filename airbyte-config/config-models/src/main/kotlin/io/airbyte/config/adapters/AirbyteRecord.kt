package io.airbyte.config.adapters

import io.airbyte.config.StreamDescriptor
import io.airbyte.protocol.models.AirbyteMessage

interface AirbyteRecord {
  val streamDescriptor: StreamDescriptor
  val asProtocol: AirbyteMessage

  fun has(fieldName: String): Boolean

  fun get(fieldName: String): Value

  fun remove(fieldName: String)

  fun <T : Any> set(
    fieldName: String,
    value: T,
  )
}

interface Value {
  fun asBoolean(): Boolean

  fun asNumber(): Number

  fun asString(): String
}
