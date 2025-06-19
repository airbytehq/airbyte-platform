/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.ThrowableProxy
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import io.airbyte.commons.logging.logback.CALLER_LINE_NUMBER_PATTERN
import io.airbyte.commons.logging.logback.CALLER_METHOD_NAME_PATTERN
import io.airbyte.commons.logging.logback.CALLER_QUALIFIED_CLASS_NAME_PATTERN
import io.airbyte.commons.logging.logback.CALLER_THREAD_NAME_PATTERN
import io.airbyte.commons.logging.logback.MaskedDataConverter
import kotlinx.coroutines.CancellationException

private val maskedDataConverter = MaskedDataConverter()

/**
 * Extension function that converts a Logback [ILoggingEvent] into an Airbyte structured [LogEvent].
 */
fun ILoggingEvent.toLogEvent(): LogEvent {
  val throwable = (this.throwableProxy as? ThrowableProxy)?.throwable
  val caller = this.getCaller()
  return LogEvent(
    timestamp = this.timeStamp,
    message = maskedDataConverter.convert(this),
    level = this.level.toString(),
    logSource = LogSource.find(this.mdcPropertyMap.getOrDefault(LOG_SOURCE_MDC_KEY, LogSource.PLATFORM.displayName)) ?: LogSource.PLATFORM,
    caller = caller,
    throwable = throwable,
  )
}

/**
 * Extension function creates a [LogCaller] from a Logback [ILoggingEvent].
 * <p>
 * If the [ILoggingEvent] contains specific keys in the MDC property map, those values are used for caller information.
 * Otherwise, values returned by the [ILoggingEvent.getCallerData] and [ILoggingEvent.getThreadName] methods are used.
 */
fun ILoggingEvent.getCaller(): LogCaller =
  LogCaller(
    className =
      this.mdcPropertyMap[CALLER_QUALIFIED_CLASS_NAME_PATTERN]
        ?: if (this.callerData.isNotEmpty()) this.callerData.first().className else null,
    methodName =
      this.mdcPropertyMap[CALLER_METHOD_NAME_PATTERN]
        ?: if (this.callerData.isNotEmpty()) this.callerData.first().methodName else null,
    lineNumber =
      this.mdcPropertyMap[CALLER_LINE_NUMBER_PATTERN]?.toInt()
        ?: if (this.callerData.isNotEmpty()) this.callerData.first().lineNumber else null,
    threadName = this.mdcPropertyMap[CALLER_THREAD_NAME_PATTERN] ?: this.threadName,
  )

/**
 * Represents the calling thread information for the log event.
 */
data class LogCaller(
  val className: String? = null,
  val methodName: String? = null,
  val lineNumber: Int? = null,
  val threadName: String? = null,
)

/**
 * Represents a structured log event.
 */
data class LogEvent(
  val timestamp: Long,
  val message: String,
  val level: String,
  val logSource: LogSource = LogSource.PLATFORM,
  val caller: LogCaller? = null,
  val throwable: Throwable? = null,
)

/**
 * Represents a collection of structured log events.
 */
data class LogEvents(
  val events: List<LogEvent>,
  val version: String = LOG_EVENT_SCHEMA_VERSION,
)

/**
 * Custom Jackson [StdDeserializer] to reduce the amount of characters written for [StackTraceElement] objects.
 */
class StackTraceElementDeserializer : StdDeserializer<StackTraceElement>(StackTraceElement::class.java) {
  override fun deserialize(
    parser: JsonParser,
    ctxt: DeserializationContext,
  ): StackTraceElement {
    val node = parser.codec.readTree<JsonNode>(parser)
    val className = node.get(SHORT_CLASS_NAME_FIELD).asText()
    val methodName = node.get(SHORT_METHOD_NAME_FIELD).asText()
    val lineNumber = node.get(SHORT_LINE_NUMBER_FIELD).asInt()
    return StackTraceElement(className, methodName, null, lineNumber)
  }
}

/**
 *  * Custom Jackson [StdSerializer] to reduce the number of characters written for [StackTraceElement] objects.
 */
class StackTraceElementSerializer : StdSerializer<StackTraceElement>(StackTraceElement::class.java) {
  override fun serialize(
    value: StackTraceElement,
    gen: JsonGenerator,
    provider: SerializerProvider,
  ) {
    with(gen) {
      writeStartObject()
      writeStringField(SHORT_CLASS_NAME_FIELD, value.className)
      writeNumberField(SHORT_LINE_NUMBER_FIELD, value.lineNumber)
      writeStringField(SHORT_METHOD_NAME_FIELD, value.methodName)
      writeEndObject()
    }
  }
}

/**
 * Custom Jackson [StdSerializer] to avoid serialization of additional members that are unnecessary/may reference classes
 * that do not exist on the classpath at runtime.
 */
class CancellationExceptionSerializer : StdSerializer<CancellationException>(CancellationException::class.java) {
  override fun serialize(
    value: CancellationException,
    gen: JsonGenerator,
    provider: SerializerProvider,
  ) {
    with(gen) {
      writeStartObject()
      writeStringField(MESSAGE_FIELD, value.message)
      writeObjectField(CAUSE_FIELD, value.cause)
      writeEndObject()
    }
  }
}

private const val CAUSE_FIELD = "cause"
private const val MESSAGE_FIELD = "message"
private const val SHORT_CLASS_NAME_FIELD = "cn"
private const val SHORT_LINE_NUMBER_FIELD = "ln"
private const val SHORT_METHOD_NAME_FIELD = "mn"
const val LOG_EVENT_SCHEMA_VERSION = "1"
