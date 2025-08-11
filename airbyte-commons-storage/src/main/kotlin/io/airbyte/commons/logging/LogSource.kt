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

/**
 * Enum representing the different sources of logs in the Airbyte platform.
 * Used to categorize and route log messages from various components.
 */
enum class LogSource(
  @JsonValue val displayName: String,
) {
  DESTINATION(displayName = DESTINATION_DISPLAY_NAME),
  PLATFORM(displayName = PLATFORM_DISPLAY_NAME),
  REPLICATION_ORCHESTRATOR(displayName = REPLICATION_ORCHESTRATOR_DISPLAY_NAME),
  SOURCE(displayName = SOURCE_DISPLAY_NAME),
  ;

  /**
   * Converts this LogSource to an MDC (Mapped Diagnostic Context) entry for logging.
   *
   * @return a map containing the log source key-value pair for MDC
   */
  fun toMdc(): Map<String, String> = mapOf(LOG_SOURCE_MDC_KEY to displayName)

  companion object {
    /**
     * Finds a LogSource by its display name (case-insensitive).
     *
     * @param displayName the display name to search for
     * @return the matching LogSource, or null if not found
     */
    @JvmStatic
    fun find(displayName: String): LogSource? =
      entries.find { it.displayName.lowercase(Locale.getDefault()) == displayName.lowercase(Locale.getDefault()) }
  }
}
