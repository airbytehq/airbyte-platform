/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.client.model.generated.JobRead
import io.airbyte.data.repositories.ConnectionTimelineEventRepository
import io.airbyte.data.repositories.entities.ConnectionTimelineEvent
import io.airbyte.data.services.ConnectionTimelineEventService
import io.airbyte.data.services.shared.ConnectionEvent
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

private val logger = KotlinLogging.logger {}

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

  /**
   * The returned associated user could be null when:
   * 1. There are no events found for the given job.
   * 2. The events found for the given job have no associated user (user_id itself is null).
   */
  override fun findAssociatedUserForAJob(
    job: JobRead,
    eventType: ConnectionEvent.Type?,
  ): UUID? {
    val connectionId = UUID.fromString(job.configId)
    val jobId = job.id
    val createdAtStart = OffsetDateTime.ofInstant(java.time.Instant.ofEpochSecond(job.createdAt), ZoneOffset.UTC)
    val userIds = repository.findAssociatedUserForAJob(connectionId, jobId, eventType, createdAtStart)
    if (userIds.isEmpty()) {
      logger.info { "No events found for connectionId: $connectionId, jobId: $jobId, eventType: $eventType, createdAtStart: $createdAtStart" }
    }
    // In case we have duped events saved for a job (a known issue we have seen in the db), we want to return the first one.
    return userIds.firstOrNull()
  }
}
