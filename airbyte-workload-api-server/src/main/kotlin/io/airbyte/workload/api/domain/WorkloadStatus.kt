package io.airbyte.workload.api.domain

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonValue
import java.util.Objects

enum class WorkloadStatus(private val value: String) {
  PENDING("pending"),
  CLAIMED("claimed"),
  RUNNING("running"),
  SUCCESS("success"),
  FAILURE("failure"),
  CANCELLED("cancelled"),
  ;

  @JsonValue
  override fun toString(): String {
    return value
  }

  companion object {
    /**
     * Convert a String into String, as specified in the
     * [See JAX RS 2.0 Specification, section 3.2, p. 12](https://download.oracle.com/otndocs/jcp/jaxrs-2_0-fr-eval-spec/index.html)
     */
    fun fromString(s: String): WorkloadStatus {
      for (b in entries) {
        // using Objects.toString() to be safe if value type non-object type
        // because types like 'int' etc. will be auto-boxed
        if (Objects.toString(b.value) == s) {
          return b
        }
      }
      throw IllegalArgumentException("Unexpected string value '$s'")
    }

    @JsonCreator
    fun fromValue(value: String): WorkloadStatus {
      for (b in entries) {
        if (b.value == value) {
          return b
        }
      }
      throw IllegalArgumentException("Unexpected value '$value'")
    }
  }
}
