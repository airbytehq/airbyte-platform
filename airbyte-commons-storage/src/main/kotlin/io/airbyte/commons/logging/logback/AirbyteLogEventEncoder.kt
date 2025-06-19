/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging.logback

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.encoder.EncoderBase
import com.fasterxml.jackson.databind.module.SimpleModule
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.commons.logging.CancellationExceptionSerializer
import io.airbyte.commons.logging.LogEvents
import io.airbyte.commons.logging.StackTraceElementSerializer
import io.airbyte.commons.logging.toLogEvent
import kotlinx.coroutines.CancellationException

val EMPTY_BYTES: ByteArray = ByteArray(0)
val NEW_LINE = "\n".toByteArray()

/**
 * Custom Logback [EncoderBase] that converts [ILoggingEvent] events into structured Airbyte [io.airbyte.commons.logging.LogEvent] events.
 */
class AirbyteLogEventEncoder : EncoderBase<ILoggingEvent>() {
  private val objectMapper = MoreMappers.initMapper()

  override fun headerBytes(): ByteArray = EMPTY_BYTES

  override fun footerBytes(): ByteArray = EMPTY_BYTES

  /**
   * Converts the list of [ILoggingEvent] events into a [io.airbyte.commons.logging.LogEvents] document.
   *
   * @param loggingEvents A list of [ILoggingEvent] events.
   * @return A JSON string representation of a [io.airbyte.commons.logging.LogEvents] document containing
   *  a structured log event for each Logback logging event.
   */
  fun bulkEncode(loggingEvents: List<ILoggingEvent>): String =
    objectMapper.writeValueAsString(LogEvents(events = loggingEvents.map(ILoggingEvent::toLogEvent)))

  override fun encode(loggingEvent: ILoggingEvent): ByteArray = objectMapper.writeValueAsBytes(loggingEvent.toLogEvent()) + NEW_LINE

  override fun start() {
    super.start()
    val structuredLogEventModule = SimpleModule()
    structuredLogEventModule.addSerializer(StackTraceElement::class.java, StackTraceElementSerializer())
    structuredLogEventModule.addSerializer(CancellationException::class.java, CancellationExceptionSerializer())
    objectMapper.registerModule(structuredLogEventModule)
  }
}
