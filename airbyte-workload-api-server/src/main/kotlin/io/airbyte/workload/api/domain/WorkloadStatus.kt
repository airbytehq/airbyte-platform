/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.domain

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonValue

enum class WorkloadStatus(
  private val value: String,
) {
  @JsonProperty("pending")
  PENDING("pending"),

  @JsonProperty("claimed")
  CLAIMED("claimed"),

  @JsonProperty("launched")
  LAUNCHED("launched"),

  @JsonProperty("running")
  RUNNING("running"),

  @JsonProperty("success")
  SUCCESS("success"),

  @JsonProperty("failure")
  FAILURE("failure"),

  @JsonProperty("cancelled")
  CANCELLED("cancelled"),
  ;

  @JsonValue
  override fun toString(): String = value

  companion object {
    @JvmStatic
    @JsonCreator
    fun fromValue(value: String): WorkloadStatus {
      val result = entries.firstOrNull { it.value.equals(value, true) }
      return result ?: throw IllegalArgumentException("Unexpected value '$value'")
    }
  }
}
