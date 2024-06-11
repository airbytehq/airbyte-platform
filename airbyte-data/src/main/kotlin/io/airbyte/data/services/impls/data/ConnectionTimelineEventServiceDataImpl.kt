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
import java.util.UUID

@Singleton
class ConnectionTimelineEventServiceDataImpl(
  private val repository: ConnectionTimelineEventRepository,
  private val mapper: ObjectMapper,
) : ConnectionTimelineEventService {
  override fun writeEvent(
    connectionId: UUID,
    event: ConnectionEvent,
  ): ConnectionTimelineEvent {
    val serializedEvent = mapper.writeValueAsString(event)
    val timelineEvent =
      ConnectionTimelineEvent(null, connectionId, event.getUserId(), event.getEventType().toString(), serializedEvent, null)
    return repository.save(timelineEvent)
  }
}
