/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.api.model.generated.JobRead
import io.airbyte.data.repositories.entities.ConnectionTimelineEvent
import io.airbyte.data.repositories.entities.ConnectionTimelineEventMinimal
import io.airbyte.data.services.shared.ConnectionEvent
import java.time.OffsetDateTime
import java.util.UUID

interface ConnectionTimelineEventService {
  // This function is used to write an event with the current timestamp. Should be used for all new events.
  fun writeEvent(
    connectionId: UUID,
    event: ConnectionEvent,
    userId: UUID? = null,
    jobId: Long? = null,
  ): ConnectionTimelineEvent

  // This function is used to write an event with a specific timestamp. This is ONLY useful for backfilling.
  fun writeEventWithTimestamp(
    connectionId: UUID,
    event: ConnectionEvent,
    userId: UUID? = null,
    createdAt: OffsetDateTime,
    jobId: Long? = null,
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

  fun listEventsForJob(jobId: Long): List<ConnectionTimelineEvent>

  fun findAssociatedUserForAJob(
    job: JobRead,
    eventType: ConnectionEvent.Type? = null,
  ): UUID?

  fun listEventsMinimal(
    connectionIds: List<UUID>,
    eventTypes: List<ConnectionEvent.Type>,
    createdAtStart: OffsetDateTime,
    createdAtEnd: OffsetDateTime,
  ): List<ConnectionTimelineEventMinimal>
}
