/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.concurrency

interface ClosableQueue<T> {
  @Throws(InterruptedException::class)
  fun poll(): T?

  @Throws(InterruptedException::class)
  fun add(e: T): Boolean

  fun size(): Int

  fun isDone(): Boolean

  fun close()

  fun isClosed(): Boolean
}
