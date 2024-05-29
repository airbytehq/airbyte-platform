package io.airbyte.workers.internal.bookkeeping.events

import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusCachingApiClient
import io.micronaut.context.event.ApplicationEventListener
import jakarta.inject.Singleton

@Singleton
class StreamStatusUpdateEventListener(
  private val streamStatusCacheClient: StreamStatusCachingApiClient,
) : ApplicationEventListener<StreamStatusUpdateEvent> {
  override fun onApplicationEvent(event: StreamStatusUpdateEvent): Unit = streamStatusCacheClient.put(event.key, event.runState)
}
