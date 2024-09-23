package io.airbyte.data.services.shared

import com.fasterxml.jackson.annotation.JsonInclude

@JsonInclude(JsonInclude.Include.NON_NULL)
class ConnectionEnabledEvent : ConnectionEvent {
  override fun getEventType(): ConnectionEvent.Type {
    return ConnectionEvent.Type.CONNECTION_ENABLED
  }
}
