package io.airbyte.workers.internal.bookkeeping.events

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.event.ApplicationEventPublisher
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

/**
 * Collection of utility methods that aid with the publishing of
 * [ReplicationAirbyteMessageEvent] messages.
 */
@Singleton
class ReplicationAirbyteMessageEventPublishingHelper(
  private val eventPublisher: ApplicationEventPublisher<ReplicationAirbyteMessageEvent>,
) {
  /**
   * Publishes a replication event.
   *
   * @param event A [ReplicationAirbyteMessageEvent].
   */
  fun publishEvent(event: ReplicationAirbyteMessageEvent) = eventPublisher.publishEvent(event)
}
