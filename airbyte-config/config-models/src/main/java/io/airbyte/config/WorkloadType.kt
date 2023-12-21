package io.airbyte.config

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonValue

enum class WorkloadType(private val value: String) {
  @JsonProperty("sync")
  SYNC("sync"),

  @JsonProperty("check")
  CHECK("check"),

  @JsonProperty("discover")
  DISCOVER("discover"),

  @JsonProperty("spec")
  SPEC("spec"),
  ;

  @JsonValue
  override fun toString(): String {
    return value
  }

  companion object {
    @JvmStatic
    @JsonCreator
    fun fromValue(value: String): WorkloadType {
      val result = entries.firstOrNull { it.value.equals(value, true) }
      return result ?: throw IllegalArgumentException("Unexpected value '$value'")
    }
  }
}
