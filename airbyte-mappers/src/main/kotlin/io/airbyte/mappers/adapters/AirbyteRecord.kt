/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.adapters

interface AirbyteRecord {
  enum class Change {
    NULLED,
    TRUNCATED,
  }

  enum class Reason {
    PLATFORM_SERIALIZATION_ERROR,
  }

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

  fun setInclude(value: Boolean)

  fun shouldInclude(): Boolean
}

interface Value {
  fun asBoolean(): Boolean

  fun asNumber(): Number

  fun asString(): String
}
