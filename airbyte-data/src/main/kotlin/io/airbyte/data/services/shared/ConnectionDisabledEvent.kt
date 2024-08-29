package io.airbyte.data.services.shared

import com.fasterxml.jackson.annotation.JsonInclude

@JsonInclude(JsonInclude.Include.NON_NULL)
class ConnectionDisabledEvent(
  private val startTimeEpochSeconds: Long,
  private val disabledReason: String? = null,
) : ConnectionEvent {
  fun getStartTimeEpochSeconds(): Long {
    return startTimeEpochSeconds
  }

  fun getDisabledReason(): String? {
    return disabledReason
  }

  override fun getEventType(): ConnectionEvent.Type {
    return ConnectionEvent.Type.CONNECTION_DISABLED
  }
}
