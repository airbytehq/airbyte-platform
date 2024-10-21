/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.data.repositories.ConnectionTimelineEventRepository
import io.airbyte.data.repositories.entities.ConnectionTimelineEvent
import io.airbyte.data.services.ConnectionTimelineEventService
import io.airbyte.data.services.shared.ConnectionEvent
import jakarta.inject.Singleton
import java.time.OffsetDateTime
import java.util.UUID

@Singleton
class ConnectionTimelineEventServiceDataImpl(
  private val repository: ConnectionTimelineEventRepository,
  private val mapper: ObjectMapper,
) : ConnectionTimelineEventService {
  override fun writeEvent(
    connectionId: UUID,
    event: ConnectionEvent,
    userId: UUID?,
  ): ConnectionTimelineEvent {
    val serializedEvent = mapper.writeValueAsString(event)
    val timelineEvent =
      ConnectionTimelineEvent(null, connectionId, userId, event.getEventType().toString(), serializedEvent, OffsetDateTime.now())
    return repository.save(timelineEvent)
  }

  override fun writeEventWithTimestamp(
    connectionId: UUID,
    event: ConnectionEvent,
    userId: UUID?,
    createdAt: OffsetDateTime,
  ): ConnectionTimelineEvent {
    val serializedEvent = mapper.writeValueAsString(event)
    val timelineEvent =
      ConnectionTimelineEvent(null, connectionId, userId, event.getEventType().toString(), serializedEvent, createdAt)
    return repository.save(timelineEvent)
  }

  override fun getEvent(eventId: UUID): ConnectionTimelineEvent {
    return repository.findById(eventId).get()
  }

  override fun listEvents(
    connectionId: UUID,
    eventTypes: List<ConnectionEvent.Type>?,
    createdAtStart: OffsetDateTime?,
    createdAtEnd: OffsetDateTime?,
    pageSize: Int,
    rowOffset: Int,
  ): List<ConnectionTimelineEvent> {
    return repository.findByConnectionIdWithFilters(connectionId, eventTypes, createdAtStart, createdAtEnd, pageSize, rowOffset)
  }
}
