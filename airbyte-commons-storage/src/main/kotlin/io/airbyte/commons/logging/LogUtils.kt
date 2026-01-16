/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import ch.qos.logback.classic.pattern.ThrowableProxyConverter
import ch.qos.logback.classic.spi.IThrowableProxy
import ch.qos.logback.classic.spi.LoggingEvent
import ch.qos.logback.classic.spi.ThrowableProxy
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import jakarta.inject.Singleton

@Singleton
class LogUtils {
  private val throwableConverter = ThrowableProxyConverter()

  @PostConstruct
  fun init() {
    throwableConverter.start()
  }

  @PreDestroy
  fun close() {
    throwableConverter.stop()
  }

  /**
   * Converts a [Throwable] object to a printed stack trace representation.
   *
   * @param throwable The [Throwable] object
   * @return A stack trace representation of the throwable.
   */
  fun convertThrowableToStackTrace(throwable: Throwable?): String? =
    throwable?.let { t ->
      val loggingEvent =
        object : LoggingEvent() {
          override fun getThrowableProxy(): IThrowableProxy = ThrowableProxy(t)
        }
      throwableConverter.convert(loggingEvent)
    }
}
