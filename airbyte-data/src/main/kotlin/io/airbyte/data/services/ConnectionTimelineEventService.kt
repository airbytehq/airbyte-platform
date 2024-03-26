package io.airbyte.data.services

import io.airbyte.data.repositories.entities.ConnectionTimelineEvent

interface ConnectionTimelineEventService {
  fun writeEvent(event: ConnectionTimelineEvent): ConnectionTimelineEvent
}
