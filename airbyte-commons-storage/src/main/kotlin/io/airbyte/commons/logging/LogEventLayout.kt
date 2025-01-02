/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import ch.qos.logback.classic.pattern.ThrowableProxyConverter
import ch.qos.logback.core.CoreConstants.LINE_SEPARATOR
import jakarta.inject.Singleton
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

/**
 * This class converts a [LogEvent] structured log message into a string
 * that can be displayed.  It is analogous to a Logback [ch.qos.logback.core.Layout].
 */
@Singleton
class LogEventLayout(val logUtils: LogUtils) {
  /**
   * Converts the provided structured [LogEvent] into a string representation of the
   * data contained in the structured event.
   *
   * @param logEvent A structured [LogEvent]
   * @return A string representation of the structured log event using formatting rules
   *  contained in this class.
   */
  fun doLayout(logEvent: LogEvent): String {
    return buildString {
      append(
        Instant.ofEpochMilli(logEvent.timestamp).atZone(UTC_ZONE_ID).format(EVENT_TIMESTAMP_FORMATTER),
      )
      append(" ")
      append("${formatLogSource(logEvent.logSource)}$MESSAGE_SEPARATOR")
      append(logEvent.message)
      logEvent.throwable?.let {
        append("$LINE_SEPARATOR${generateStackTrace(throwable = logEvent.throwable)}")
      }
      append(LINE_SEPARATOR)
    }
  }

  /**
   * Converts a [Throwable] to printable stack trace.
   *
   * @param throwable A [Throwable]
   * @return A string representation of a stack trace derived from the provided throwable
   * @see [ThrowableProxyConverter.convert]
   */
  fun generateStackTrace(throwable: Throwable): String? {
    return logUtils.convertThrowableToStackTrace(throwable = throwable)
  }

  /**
   * Formats the log source by applying the appropriate ANSI color.
   *
   * @param logSource The log source associated with the event.
   * @return The formatted log source display name.
   */
  fun formatLogSource(logSource: LogSource?): String {
    return when (logSource) {
      LogSource.DESTINATION -> applyColor(Color.YELLOW, logSource.displayName)
      LogSource.PLATFORM -> applyColor(Color.CYAN, logSource.displayName)
      LogSource.REPLICATION_ORCHESTRATOR -> applyColor(Color.CYAN, logSource.displayName)
      LogSource.SOURCE -> applyColor(Color.BLUE, logSource.displayName)
      null -> applyColor(Color.CYAN, LogSource.PLATFORM.displayName)
    }
  }

  private fun applyColor(
    color: Color,
    text: String,
  ): String {
    return "${color.code}$text$RESET"
  }

  enum class Color(val code: String) {
    BLUE("\u001b[44m"),
    YELLOW("\u001b[43m"),
    CYAN("\u001b[46m"),
  }
}

private const val LOG_EVENT_TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss"
val UTC_ZONE_ID: ZoneId = ZoneId.of("UTC")
val EVENT_TIMESTAMP_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern(LOG_EVENT_TIMESTAMP_PATTERN)
const val MESSAGE_SEPARATOR = " > "
private const val RESET: String = "\u001B[0m"
