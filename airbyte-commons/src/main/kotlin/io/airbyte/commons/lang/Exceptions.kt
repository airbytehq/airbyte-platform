/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.lang

import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.Callable
import java.util.function.Function

/**
 * Shared code for handling [Exception].
 */
@Deprecated("")
object Exceptions {
  private val log = KotlinLogging.logger {}

  /**
   * Catch a checked exception and rethrow as a [RuntimeException].
   *
   * @param callable - function that throws a checked exception.
   * @param <T> - return type of the function.
   * @return object that the function returns.
   </T> */
  @JvmStatic
  fun <T> toRuntime(callable: Callable<T>): T {
    try {
      return callable.call()
    } catch (e: java.lang.RuntimeException) {
      throw e
    } catch (e: Exception) {
      throw java.lang.RuntimeException(e)
    }
  }

  /**
   * Catch a checked exception and rethrow as a [RuntimeException].
   *
   * @param voidCallable - function that throws a checked exception.
   */
  @JvmStatic
  fun toRuntime(voidCallable: Procedure) {
    castCheckedToRuntime(voidCallable) { cause: Exception -> RuntimeException(cause) }
  }

  private fun castCheckedToRuntime(
    voidCallable: Procedure,
    exceptionFactory: Function<Exception, java.lang.RuntimeException>,
  ) {
    try {
      voidCallable.call()
    } catch (e: java.lang.RuntimeException) {
      throw e
    } catch (e: Exception) {
      throw exceptionFactory.apply(e)
    }
  }

  /**
   * Swallow an exception and log it to STDERR.
   *
   * @param procedure code that emits exception to swallow.
   */
  @JvmStatic
  fun swallow(procedure: () -> Unit) {
    try {
      procedure()
    } catch (e: Exception) {
      log.error("Swallowed error.", e)
    }
  }

  /**
   * Swallow [Exception] and returns a default value.
   *
   * @param procedure code that emits exception to swallow.
   * @param defaultValue value to return when an exception is thrown.
   * @param <T> type of the value returned.
   * @return value from the wrapped code or the default value if an exception is thrown.
   </T> */
  fun <T> swallowWithDefault(
    procedure: () -> T,
    defaultValue: T,
  ): T {
    try {
      return procedure()
    } catch (e: Exception) {
      return defaultValue
    }
  }

  /**
   * Abstraction for swallowing exceptions.
   */
  interface Procedure {
    @Throws(Exception::class)
    fun call()
  }
}
