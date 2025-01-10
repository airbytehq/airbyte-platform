/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging.logback

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.pattern.ClassOfCallerConverter
import ch.qos.logback.classic.pattern.LineOfCallerConverter
import ch.qos.logback.classic.pattern.MethodOfCallerConverter
import ch.qos.logback.classic.pattern.ThreadConverter
import ch.qos.logback.classic.pattern.ThrowableProxyConverter
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.CoreConstants.DASH_CHAR
import ch.qos.logback.core.CoreConstants.ISO8601_PATTERN
import ch.qos.logback.core.CoreConstants.LINE_SEPARATOR
import ch.qos.logback.core.CoreConstants.TAB
import ch.qos.logback.core.LayoutBase
import ch.qos.logback.core.pattern.color.ANSIConstants
import ch.qos.logback.core.pattern.color.ANSIConstants.DEFAULT_FG
import ch.qos.logback.core.pattern.color.ANSIConstants.ESC_END
import ch.qos.logback.core.pattern.color.ANSIConstants.ESC_START
import ch.qos.logback.core.pattern.color.ANSIConstants.RESET
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

/**
 * Custom Logback message layout that formats the message for platform log messages (e.g. STDOUT).
 */
class AirbytePlatformLogbackMessageLayout : LayoutBase<ILoggingEvent>() {
  private val classOfCallerConverter = ClassOfCallerConverter()
  private val lineOfCallerConverter = LineOfCallerConverter()
  private val methodOfCallerConverter = MethodOfCallerConverter()
  private val threadConverter = ThreadConverter()
  private val throwableConverter = ThrowableProxyConverter()
  private val maskedDataConverter = MaskedDataConverter()
  private val ciMode = System.getProperty(CI_MODE_SYSTEM_PROPERTY, "false").toBoolean()

  init {
    throwableConverter.start()
  }

  override fun doLayout(loggingEvent: ILoggingEvent): String =
    buildString {
      append(
        Instant.ofEpochMilli(loggingEvent.timeStamp).atZone(UTC_ZONE_ID).format(EVENT_TIMESTAMP_FORMATTER),
      )

      append(" ")

      /*
       * Add DataDog trace/span ID's to log messages if CI mode is enabled and the log
       * message is not for the job log.
       */
      if (ciMode) {
        append(
          "[dd.trace_id=${loggingEvent.mdcPropertyMap[DATADOG_TRACE_ID_KEY]} " +
            "dd.span_id=${loggingEvent.mdcPropertyMap[DATADOG_SPAN_ID_KEY]}] ",
        )
      }

      append("[")
      if (loggingEvent.mdcPropertyMap.containsKey(CALLER_THREAD_NAME_PATTERN)) {
        append(loggingEvent.mdcPropertyMap[CALLER_THREAD_NAME_PATTERN])
      } else {
        append(threadConverter.convert(loggingEvent))
      }
      append("]$TAB")
      append("$ESC_START${getHighlightColor(loggingEvent = loggingEvent)}$ESC_END${loggingEvent.level}$DEFAULT_COLOR$TAB")
      if (loggingEvent.mdcPropertyMap.containsKey(CALLER_QUALIFIED_CLASS_NAME_PATTERN)) {
        append(
          "${formatClassName(loggingEvent.mdcPropertyMap[CALLER_QUALIFIED_CLASS_NAME_PATTERN])}" +
            "(${loggingEvent.mdcPropertyMap[CALLER_METHOD_NAME_PATTERN]}):" +
            "${loggingEvent.mdcPropertyMap[CALLER_LINE_NUMBER_PATTERN]} $DASH_CHAR ",
        )
      } else {
        append(
          "${formatClassName(classOfCallerConverter.convert(loggingEvent))}(${methodOfCallerConverter.convert(loggingEvent)})" +
            ":${lineOfCallerConverter.convert(loggingEvent)} $DASH_CHAR ",
        )
      }
      append(maskedDataConverter.convert(event = loggingEvent))
      loggingEvent.throwableProxy?.let {
        append("$LINE_SEPARATOR${throwableConverter.convert(loggingEvent)}")
      }
      append(LINE_SEPARATOR)
    }
}

internal const val DEFAULT_COLOR = ESC_START + RESET + DEFAULT_FG + ESC_END
internal const val CI_MODE_SYSTEM_PROPERTY = "ciMode"
internal const val DATADOG_SPAN_ID_KEY = "dd.span_id"
internal const val DATADOG_TRACE_ID_KEY = "dd.trace_id"
private val EVENT_TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern(ISO8601_PATTERN)
private val UTC_ZONE_ID = ZoneId.of("UTC")

/**
 * Formats the fully qualified class name to mimic the same behavior as the ``{1.}`` option
 * in a Log4j pattern layout format string.
 *
 * @param className a fully qualified class name
 * @returns The formatted fully qualified class name.
 */
internal fun formatClassName(className: String?): String? {
  return className?.let {
    val parts = className.split('.')
    return "${parts.subList(0, parts.size - 1).joinToString(".") { s -> s.substring(0, 1) }}.${parts.last()}"
  }
}

/**
 * Returns the appropriate highlight color based on the level associated with the provided logging event.
 * This method is adapted from [ch.qos.logback.classic.pattern.color.HighlightingCompositeConverter] used
 * by Logback to color levels in log output.
 *
 * @param loggingEvent The logging event that contains the log level.
 * @return The ANSI color code associated with the log level.
 */
private fun getHighlightColor(loggingEvent: ILoggingEvent): String =
  when (loggingEvent.level.toInt()) {
    Level.ERROR_INT -> ANSIConstants.BOLD + ANSIConstants.RED_FG
    Level.WARN_INT -> ANSIConstants.RED_FG
    Level.INFO_INT -> ANSIConstants.BLUE_FG
    else -> DEFAULT_FG
  }
