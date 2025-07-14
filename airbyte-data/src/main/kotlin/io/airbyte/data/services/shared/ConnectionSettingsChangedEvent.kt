/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared

import com.fasterxml.jackson.annotation.JsonInclude

@JsonInclude(JsonInclude.Include.NON_NULL)
class ConnectionSettingsChangedEvent(
  private val patches: Map<String, Map<String, Any?>>,
  private val updateReason: String? = null,
) : ConnectionEvent {
  fun getPatches(): Map<String, Map<String, Any?>> = patches

  fun getUpdateReason(): String? = updateReason

  override fun getEventType(): ConnectionEvent.Type = ConnectionEvent.Type.CONNECTION_SETTINGS_UPDATE
}
