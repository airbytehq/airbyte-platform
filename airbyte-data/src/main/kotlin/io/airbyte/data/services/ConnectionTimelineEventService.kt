package io.airbyte.data.services

import io.airbyte.data.repositories.entities.ConnectionTimelineEvent
import io.airbyte.data.services.shared.ConnectionEvent
import java.time.OffsetDateTime
import java.util.UUID

interface ConnectionTimelineEventService {
  fun writeEvent(
    connectionId: UUID,
    event: ConnectionEvent,
    userId: UUID? = null,
  ): ConnectionTimelineEvent

  fun getEvent(eventId: UUID): ConnectionTimelineEvent

  fun listEvents(
    connectionId: UUID,
    eventTypes: List<ConnectionEvent.Type>? = null,
    createdAtStart: OffsetDateTime? = null,
    createdAtEnd: OffsetDateTime? = null,
    pageSize: Int,
    rowOffset: Int,
  ): List<ConnectionTimelineEvent>
}
