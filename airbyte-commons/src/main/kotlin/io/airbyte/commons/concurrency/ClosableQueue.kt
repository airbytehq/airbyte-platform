/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.concurrency

interface ClosableQueue<T> {
  fun poll(): T?

  fun add(e: T): Boolean

  fun size(): Int

  fun isDone(): Boolean

  fun close()

  fun isClosed(): Boolean
}
