/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping.events

import io.micronaut.context.event.ApplicationEventPublisher
import jakarta.inject.Singleton

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
