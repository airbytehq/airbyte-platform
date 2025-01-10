/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import com.fasterxml.jackson.annotation.JsonValue
import java.util.Locale

private const val DESTINATION_DISPLAY_NAME = "destination"
private const val SOURCE_DISPLAY_NAME = "source"
private const val PLATFORM_DISPLAY_NAME = "platform"
private const val REPLICATION_ORCHESTRATOR_DISPLAY_NAME = "replication-orchestrator"
const val LOG_SOURCE_MDC_KEY = "log_source"

enum class LogSource(
  @JsonValue val displayName: String,
) {
  DESTINATION(displayName = DESTINATION_DISPLAY_NAME),
  PLATFORM(displayName = PLATFORM_DISPLAY_NAME),
  REPLICATION_ORCHESTRATOR(displayName = REPLICATION_ORCHESTRATOR_DISPLAY_NAME),
  SOURCE(displayName = SOURCE_DISPLAY_NAME),
  ;

  fun toMdc(): Map<String, String> {
    return mapOf(LOG_SOURCE_MDC_KEY to displayName)
  }

  companion object {
    @JvmStatic
    fun find(displayName: String): LogSource? {
      return entries.find { it.displayName.lowercase(Locale.getDefault()) == displayName.lowercase(Locale.getDefault()) }
    }
  }
}
