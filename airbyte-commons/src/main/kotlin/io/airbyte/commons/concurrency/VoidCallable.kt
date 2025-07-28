/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.concurrency

import java.util.concurrent.Callable

/**
 * Callable that has no return. Useful for coercing the same behavior for lambdas that have a return
 * value and those that don't.
 */
@FunctionalInterface
interface VoidCallable : Callable<Void?> {
  @Throws(Exception::class)
  override fun call(): Void? {
    voidCall()
    return null
  }

  @Throws(Exception::class)
  fun voidCall()
}
