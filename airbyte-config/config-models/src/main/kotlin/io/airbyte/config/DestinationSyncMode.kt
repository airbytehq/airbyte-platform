/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonValue

/**
 * DestinationSyncMode
 *
 *
 * destination sync modes.
 */
enum class DestinationSyncMode(
  private val value: String,
) {
  APPEND("append"),
  OVERWRITE("overwrite"),
  APPEND_DEDUP("append_dedup"),
  OVERWRITE_DEDUP("overwrite_dedup"),
  UPDATE("update"),
  SOFT_DELETE("soft_delete"),
  ;

  override fun toString(): String = this.value

  @JsonValue
  fun value(): String = this.value

  companion object {
    private val CONSTANTS: MutableMap<String, DestinationSyncMode> = HashMap()

    init {
      for (c in entries) {
        CONSTANTS[c.value] = c
      }
    }

    @JsonCreator
    fun fromValue(value: String): DestinationSyncMode {
      val constant = CONSTANTS[value]
      requireNotNull(constant != null) { value }
      return constant!!
    }
  }
}
