package io.airbyte.data.services.shared

import com.fasterxml.jackson.annotation.JsonInclude

@JsonInclude(JsonInclude.Include.NON_NULL)
class ConnectionSettingsChangedEvent(
  private val patches: Map<String, Map<String, Object>>,
  private val updateReason: String? = null,
) : ConnectionEvent {
  fun getPatches(): Map<String, Map<String, Object>> {
    return patches
  }

  fun getUpdateReason(): String? {
    return updateReason
  }

  override fun getEventType(): ConnectionEvent.Type {
    return ConnectionEvent.Type.CONNECTION_SETTINGS_UPDATE
  }
}
