/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.queue

/**
 * MessageConsumer interface for a temporal queue.
 */
interface MessageConsumer<T : Any> {
  fun consume(input: T)
}
