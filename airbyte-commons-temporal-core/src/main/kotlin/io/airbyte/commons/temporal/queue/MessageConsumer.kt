package io.airbyte.commons.temporal.queue

/**
 * MessageConsumer interface for a temporal queue.
 */
interface MessageConsumer<T : Any> {
  fun consume(input: T)
}
