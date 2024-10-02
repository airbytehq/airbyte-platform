/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging.logback

import ch.qos.logback.classic.pattern.ThrowableProxyConverter
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.CoreConstants.LINE_SEPARATOR
import ch.qos.logback.core.LayoutBase
import io.airbyte.commons.logging.LoggingHelper
import io.airbyte.commons.logging.LoggingHelper.LOG_SOURCE_MDC_KEY
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
    val logSource = loggingEvent.mdcPropertyMap.getOrDefault(LOG_SOURCE_MDC_KEY, LoggingHelper.platformLogSource())

    return buildString {
      append(
        Instant.ofEpochMilli(loggingEvent.timeStamp).atZone(UTC_ZONE_ID).format(EVENT_TIMESTAMP_FORMATTER),
      )
      append(" ")
      append("$logSource$MESSAGE_SEPARATOR")
      append(maskedDataConverter.convert(event = loggingEvent))
      loggingEvent.throwableProxy?.let {
        append("$LINE_SEPARATOR${throwableConverter.convert(loggingEvent)}")
      }
      append(LINE_SEPARATOR)
    }
  }
}

private val UTC_ZONE_ID = ZoneId.of("UTC")
private val EVENT_TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
const val MESSAGE_SEPARATOR = " > "
