/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging.logback

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.boolex.EventEvaluatorBase

/**
 * Custom Logback [EventEvaluatorBase] that evaluates the presence of a particular context key
 * in the [ILoggingEvent]'s MDC.  This class is necessary because as of Logback 1.5.13, the
 * JaninoEvaluator has been removed with no replacement for security reasons.
 */
class AirbyteMdcEvaluator(private val contextKey: String) : EventEvaluatorBase<ILoggingEvent>() {
  override fun evaluate(event: ILoggingEvent): Boolean = event.mdcPropertyMap[contextKey].isNullOrEmpty()
}
