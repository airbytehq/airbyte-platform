/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonValue

enum class WorkloadPriority(
  private val value: String,
) {
  @JsonProperty("high")
  HIGH("high"),

  @JsonProperty("default")
  DEFAULT("default"),
  ;

  @JsonValue
  override fun toString(): String = value

  fun toInt(): Int =
    when (this) {
      DEFAULT -> 0
      HIGH -> 1
    }

  companion object {
    @JvmStatic
    @JsonCreator
    fun fromValue(value: String): WorkloadPriority {
      val result = entries.firstOrNull { it.value.equals(value, true) }
      return result ?: throw IllegalArgumentException("Unexpected value '$value'")
    }

    @JvmStatic
    fun fromInt(value: Int): WorkloadPriority =
      when (value) {
        0 -> DEFAULT
        1 -> HIGH
        else -> throw IllegalArgumentException("Unexpected value '$value'")
      }
  }
}
