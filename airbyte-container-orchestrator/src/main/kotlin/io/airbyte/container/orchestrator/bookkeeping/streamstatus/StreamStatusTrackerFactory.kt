/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping.streamstatus

import io.airbyte.container.orchestrator.bookkeeping.events.StreamStatusUpdateEvent
import io.airbyte.workers.context.ReplicationContext
import io.airbyte.workers.helper.AirbyteMessageDataExtractor
import io.micronaut.context.event.ApplicationEventPublisher
import jakarta.inject.Singleton

/**
 * Because Docker does not use a short-lived orchestrator app per sync,
 * we must use a factory to create a StreamStatusTracker per sync to
 * isolate the internal state (replication context and status state store)
 * per instance.
 */
@Singleton
class StreamStatusTrackerFactory(
  private val dataExtractor: AirbyteMessageDataExtractor,
  private val eventPublisher: ApplicationEventPublisher<StreamStatusUpdateEvent>,
) {
  fun create(ctx: ReplicationContext): StreamStatusTracker =
    StreamStatusTracker(
      dataExtractor,
      StreamStatusStateStore(),
      eventPublisher,
      ctx,
    )
}
