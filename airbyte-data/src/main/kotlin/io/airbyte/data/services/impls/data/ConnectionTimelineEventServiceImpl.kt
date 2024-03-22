package io.airbyte.data.services.impls.data

import io.airbyte.data.repositories.ConnectionTimelineEventRepository
import io.airbyte.data.repositories.entities.ConnectionTimelineEvent
import io.airbyte.data.services.ConnectionTimelineEventService
import jakarta.inject.Singleton

@Singleton
class ConnectionTimelineEventServiceImpl(private val repository: ConnectionTimelineEventRepository) : ConnectionTimelineEventService {
  override fun writeEvent(event: ConnectionTimelineEvent): ConnectionTimelineEvent {
    return repository.save(event)
  }
}
