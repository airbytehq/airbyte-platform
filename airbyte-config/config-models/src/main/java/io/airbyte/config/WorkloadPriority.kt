package io.airbyte.config

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonValue

enum class WorkloadPriority(private val value: String) {
  @JsonProperty("high")
  HIGH("high"),

  @JsonProperty("default")
  DEFAULT("default"),
  ;

  @JsonValue
  override fun toString(): String {
    return value
  }

  companion object {
    @JvmStatic
    @JsonCreator
    fun fromValue(value: String): WorkloadPriority {
      val result = values().firstOrNull { it.value.equals(value, true) }
      return result ?: throw IllegalArgumentException("Unexpected value '$value'")
    }
  }
}
