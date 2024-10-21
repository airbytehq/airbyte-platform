package io.airbyte.config.adapters

import io.airbyte.config.StreamDescriptor
import io.airbyte.protocol.models.AirbyteMessage

interface AirbyteRecord {
  enum class Change {
    NULLED,
    TRUNCATED,
  }

  enum class Reason {
    PLATFORM_SERIALIZATION_ERROR,
  }

  val streamDescriptor: StreamDescriptor
  val asProtocol: AirbyteMessage

  fun has(fieldName: String): Boolean

  fun get(fieldName: String): Value

  fun remove(fieldName: String)

  fun rename(
    oldFieldName: String,
    newFieldName: String,
  )

  fun <T : Any> set(
    fieldName: String,
    value: T,
  )

  fun trackFieldError(
    fieldName: String,
    change: Change,
    reason: Reason,
  )
}

interface Value {
  fun asBoolean(): Boolean

  fun asNumber(): Number

  fun asString(): String
}
