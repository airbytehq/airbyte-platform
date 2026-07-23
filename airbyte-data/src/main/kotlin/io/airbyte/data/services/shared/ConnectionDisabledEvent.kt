/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared

import com.fasterxml.jackson.annotation.JsonInclude

@JsonInclude(JsonInclude.Include.NON_NULL)
class ConnectionDisabledEvent(
  private val disabledReason: String? = null,
) : ConnectionEvent {
  fun getDisabledReason(): String? = disabledReason

  override fun getEventType(): ConnectionEvent.Type = ConnectionEvent.Type.CONNECTION_DISABLED
}
