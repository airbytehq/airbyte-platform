package io.airbyte.data.services.shared

import com.fasterxml.jackson.annotation.JsonInclude

@JsonInclude(JsonInclude.Include.NON_NULL)
class ConnectionEnabledEvent(
  private val startTimeEpochSeconds: Long,
) : ConnectionEvent {
  fun getStartTimeEpochSeconds(): Long {
    return startTimeEpochSeconds
  }

  override fun getEventType(): ConnectionEvent.Type {
    return ConnectionEvent.Type.CONNECTION_ENABLED
  }
}
