package io.airbyte.data.services

import io.airbyte.data.repositories.entities.ConnectionTimelineEvent
import io.airbyte.data.services.shared.ConnectionEvent
import java.util.UUID

interface ConnectionTimelineEventService {
  fun writeEvent(
    connectionId: UUID,
    event: ConnectionEvent,
  ): ConnectionTimelineEvent
}
