/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging.logback

import ch.qos.logback.classic.pattern.ThrowableProxyConverter
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.CoreConstants.LINE_SEPARATOR
import ch.qos.logback.core.LayoutBase
import io.airbyte.commons.logging.LOG_SOURCE_MDC_KEY
import io.airbyte.commons.logging.LogSource
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

/**
 * Custom Logback message layout that formats the message for operations job log messages.
 */
class AirbyteOperationsJobLogbackMessageLayout : LayoutBase<ILoggingEvent>() {
  private val throwableConverter = ThrowableProxyConverter()
  private val maskedDataConverter = MaskedDataConverter()

  init {
    throwableConverter.start()
  }

  override fun doLayout(loggingEvent: ILoggingEvent): String {
    val logSource = loggingEvent.mdcPropertyMap.getOrDefault(LOG_SOURCE_MDC_KEY, LogSource.PLATFORM.displayName)

    return buildString {
      append(
        Instant.ofEpochMilli(loggingEvent.timeStamp).atZone(UTC_ZONE_ID).format(EVENT_TIMESTAMP_FORMATTER),
      )
      append(" ")
      append("${formatLogSource(logSource)}$MESSAGE_SEPARATOR")
      append(maskedDataConverter.convert(event = loggingEvent))
      loggingEvent.throwableProxy?.let {
        append("$LINE_SEPARATOR${throwableConverter.convert(loggingEvent)}")
      }
      append(LINE_SEPARATOR)
    }
  }

  fun formatLogSource(logSourceDisplayName: String): String {
    val logSource = LogSource.find(logSourceDisplayName)
    return when (logSource) {
      LogSource.DESTINATION -> applyColor(Color.YELLOW, logSourceDisplayName)
      LogSource.PLATFORM -> applyColor(Color.CYAN, logSourceDisplayName)
      LogSource.REPLICATION_ORCHESTRATOR -> applyColor(Color.CYAN, logSourceDisplayName)
      LogSource.SOURCE -> applyColor(Color.BLUE, logSourceDisplayName)
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

private val UTC_ZONE_ID = ZoneId.of("UTC")
private val EVENT_TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
const val MESSAGE_SEPARATOR = " > "
private const val RESET: String = "\u001B[0m"
