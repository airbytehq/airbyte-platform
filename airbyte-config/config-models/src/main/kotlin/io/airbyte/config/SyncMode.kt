/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonValue

/**
 * SyncMode
 *
 *
 * sync modes.
 */
enum class SyncMode(
  private val value: String,
) {
  FULL_REFRESH("full_refresh"),
  INCREMENTAL("incremental"),
  ;

  override fun toString(): String = this.value

  @JsonValue
  fun value(): String = this.value

  companion object {
    private val CONSTANTS: MutableMap<String, SyncMode> = HashMap()

    init {
      for (c in entries) {
        CONSTANTS[c.value] = c
      }
    }

    @JsonCreator
    fun fromValue(value: String): SyncMode {
      val constant = CONSTANTS[value]
      requireNotNull(constant != null) { value }
      return constant!!
    }
  }
}
