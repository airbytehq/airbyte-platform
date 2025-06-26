/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping.events

import io.airbyte.container.orchestrator.bookkeeping.streamstatus.StreamStatusCachingApiClient
import io.micronaut.context.event.ApplicationEventListener
import jakarta.inject.Singleton

@Singleton
class StreamStatusUpdateEventListener(
  private val streamStatusCacheClient: StreamStatusCachingApiClient,
) : ApplicationEventListener<StreamStatusUpdateEvent> {
  override fun onApplicationEvent(event: StreamStatusUpdateEvent): Unit =
    streamStatusCacheClient.put(event.cache, event.key, event.runState, event.metadata, event.ctx)
}
