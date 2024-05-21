package io.airbyte.data.services.impls.data

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.data.repositories.ConnectionTimelineEventRepository
import io.airbyte.data.repositories.entities.ConnectionTimelineEvent
import io.airbyte.data.services.ConnectionTimelineEventService
import io.airbyte.data.services.shared.ConnectionEvent
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class ConnectionTimelineEventServiceDataImpl(private val repository: ConnectionTimelineEventRepository) : ConnectionTimelineEventService {
  override fun writeEvent(
    connectionId: UUID,
    connectionEvent: ConnectionEvent,
  ): ConnectionTimelineEvent {
    val serializedEvent = MAPPER.writeValueAsString(connectionEvent)
    val event =
      ConnectionTimelineEvent(null, connectionId, connectionEvent.getUserId(), connectionEvent.getEventType().toString(), serializedEvent, null)
    return repository.save(event)
  }

  companion object {
    private val MAPPER = ObjectMapper()
  }
}
