/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.ConnectionTimelineEvent
import io.airbyte.data.services.shared.ConnectionEvent
import io.airbyte.db.instance.configs.jooq.generated.Keys
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

@MicronautTest
internal class ConnectionTimelineEventRepositoryTest : AbstractConfigRepositoryTest() {
  companion object {
    @BeforeAll
    @JvmStatic
    fun setup() {
      // Temporarily remove foreign key constraints, so we don't have to deal with making connections and users as well
      jooqDslContext
        .alterTable(
          Tables.CONNECTION_TIMELINE_EVENT,
        ).dropForeignKey(Keys.CONNECTION_TIMELINE_EVENT__CONNECTION_TIMELINE_EVENT_CONNECTION_ID_FKEY.constraint())
        .execute()
      jooqDslContext
        .alterTable(
          Tables.CONNECTION_TIMELINE_EVENT,
        ).dropForeignKey(Keys.CONNECTION_TIMELINE_EVENT__CONNECTION_TIMELINE_EVENT_USER_ID_FKEY.constraint())
        .execute()
    }
  }

  @Test
  fun `test db insertion`() {
    val initialCount = connectionTimelineEventRepository.count()
    val event =
      ConnectionTimelineEvent(
        connectionId = UUID.randomUUID(),
        eventType = "Test",
        createdAt = OffsetDateTime.now(),
      )

    val saved = connectionTimelineEventRepository.save(event)
    assert(connectionTimelineEventRepository.count() == initialCount + 1)

    val persistedEvent = connectionTimelineEventRepository.findById(saved.id!!).get()
    assert(persistedEvent.connectionId == event.connectionId)

    connectionTimelineEventRepository.deleteById(saved.id!!)
  }

  @Test
  fun `test db insertion with jobId`() {
    val initialCount = connectionTimelineEventRepository.count()
    val jobId = 12345L
    val event =
      ConnectionTimelineEvent(
        connectionId = UUID.randomUUID(),
        eventType = "Test",
        createdAt = OffsetDateTime.now(),
        jobId = jobId,
      )

    val saved = connectionTimelineEventRepository.save(event)
    assert(connectionTimelineEventRepository.count() == initialCount + 1)

    val persistedEvent = connectionTimelineEventRepository.findById(saved.id!!).get()
    assert(persistedEvent.connectionId == event.connectionId)
    assert(persistedEvent.jobId == jobId)

    connectionTimelineEventRepository.deleteById(saved.id!!)
  }

  @Nested
  inner class ListEventsTest {
    private val connectionId: UUID = UUID.randomUUID()
    private val secondConnectionId: UUID = UUID.randomUUID()
    private val event1 =
      ConnectionTimelineEvent(
        connectionId = connectionId,
        eventType = ConnectionEvent.Type.SYNC_STARTED.name,
        createdAt = OffsetDateTime.of(2024, 9, 1, 0, 0, 0, 0, ZoneOffset.UTC),
      )
    private val event2 =
      ConnectionTimelineEvent(
        connectionId = connectionId,
        eventType = ConnectionEvent.Type.SYNC_CANCELLED.name,
        createdAt = OffsetDateTime.of(2024, 9, 2, 0, 0, 0, 0, ZoneOffset.UTC),
      )
    private val event3 =
      ConnectionTimelineEvent(
        connectionId = connectionId,
        eventType = ConnectionEvent.Type.REFRESH_STARTED.name,
        createdAt = OffsetDateTime.of(2024, 9, 3, 0, 0, 0, 0, ZoneOffset.UTC),
      )
    private val event4 =
      ConnectionTimelineEvent(
        connectionId = connectionId,
        eventType = ConnectionEvent.Type.REFRESH_SUCCEEDED.name,
        createdAt = OffsetDateTime.of(2024, 9, 4, 0, 0, 0, 0, ZoneOffset.UTC),
      )
    private val event5 =
      ConnectionTimelineEvent(
        connectionId = secondConnectionId,
        eventType = ConnectionEvent.Type.REFRESH_SUCCEEDED.name,
        createdAt = OffsetDateTime.of(2024, 9, 3, 0, 0, 0, 0, ZoneOffset.UTC),
      )
    private val event6 =
      ConnectionTimelineEvent(
        connectionId = secondConnectionId,
        eventType = ConnectionEvent.Type.SYNC_SUCCEEDED.name,
        createdAt = OffsetDateTime.of(2024, 9, 4, 0, 0, 0, 0, ZoneOffset.UTC),
      )

    @BeforeEach
    fun setup() {
      // save some events
      val allEvents = listOf(event1, event2, event3, event4, event5, event6)
      allEvents.forEach { event -> connectionTimelineEventRepository.save(event) }
    }

    @AfterEach
    fun reset() {
      connectionTimelineEventRepository.deleteAll()
    }

    @Test
    fun `should list ALL events order by timestamp`() {
      val res =
        connectionTimelineEventRepository.findByConnectionIdWithFilters(
          connectionId = connectionId,
          eventTypes = null,
          createdAtStart = null,
          createdAtEnd = null,
          pageSize = 200,
          rowOffset = 0,
        )
      assert(connectionTimelineEventRepository.count() == 6L)
      assert(res.size == 4)
      assert(res[0].id == event4.id)
    }

    @Test
    fun `should list STARTED events only`() {
      val res =
        connectionTimelineEventRepository.findByConnectionIdWithFilters(
          connectionId = connectionId,
          eventTypes =
            listOf(
              ConnectionEvent.Type.SYNC_STARTED,
              ConnectionEvent.Type.REFRESH_STARTED,
              ConnectionEvent.Type.CLEAR_STARTED,
            ),
          createdAtStart = null,
          createdAtEnd = null,
          pageSize = 200,
          rowOffset = 0,
        )
      assert(res.size == 2)
    }

    @Test
    fun `should list events after given time range`() {
      val allEvents =
        connectionTimelineEventRepository.findByConnectionIdWithFilters(
          connectionId = connectionId,
          eventTypes = null,
          createdAtStart = null,
          createdAtEnd = null,
          pageSize = 200,
          rowOffset = 0,
        )
      val res =
        connectionTimelineEventRepository.findByConnectionIdWithFilters(
          connectionId = connectionId,
          eventTypes = null,
          createdAtStart = allEvents[2].createdAt,
          createdAtEnd = null,
          pageSize = 200,
          rowOffset = 0,
        )
      assert(res.size == 3)
    }

    @Test
    fun `should list events between a given time range`() {
      val allEvents =
        connectionTimelineEventRepository.findByConnectionIdWithFilters(
          connectionId = connectionId,
          eventTypes = null,
          createdAtStart = null,
          createdAtEnd = null,
          pageSize = 200,
          rowOffset = 0,
        )
      val res =
        connectionTimelineEventRepository.findByConnectionIdWithFilters(
          connectionId = connectionId,
          eventTypes = null,
          createdAtStart = allEvents[2].createdAt,
          createdAtEnd = allEvents[1].createdAt,
          pageSize = 200,
          rowOffset = 0,
        )
      assert(res.size == 2)
    }

    @Test
    fun `should list events with limit`() {
      val res =
        connectionTimelineEventRepository.findByConnectionIdWithFilters(
          connectionId = connectionId,
          eventTypes = null,
          createdAtStart = null,
          createdAtEnd = null,
          pageSize = 1,
          rowOffset = 0,
        )
      assert(res.size == 1)
    }

    @Test
    fun `should list events with row offset`() {
      val res =
        connectionTimelineEventRepository.findByConnectionIdWithFilters(
          connectionId = connectionId,
          eventTypes = null,
          createdAtStart = null,
          createdAtEnd = null,
          pageSize = 200,
          rowOffset = 2,
        )
      assert(res.size == 2)
    }

    @Test
    fun `should list events with all combined restrictions`() {
      val allEvents = // sorted by createdAtStart DESC: [event4, e3, e2, e1]
        connectionTimelineEventRepository.findByConnectionIdWithFilters(
          connectionId = connectionId,
          eventTypes = null,
          createdAtStart = null,
          createdAtEnd = null,
          pageSize = 200,
          rowOffset = 0,
        )
      val res =
        connectionTimelineEventRepository.findByConnectionIdWithFilters(
          connectionId = connectionId,
          eventTypes =
            listOf(
              // e1
              ConnectionEvent.Type.SYNC_STARTED,
              // e2
              ConnectionEvent.Type.SYNC_CANCELLED,
              // e3
              ConnectionEvent.Type.REFRESH_STARTED,
              // e4
              ConnectionEvent.Type.REFRESH_SUCCEEDED,
              // no event
              ConnectionEvent.Type.CLEAR_STARTED,
            ),
          // e1.createdAt
          createdAtStart = allEvents[3].createdAt,
          // e3.createdAt
          createdAtEnd = allEvents[1].createdAt,
          // now we should list total 3 events: [e3, e2, e1]
          pageSize = 200,
          // now we should list total 2 events: [e2, e1]
          rowOffset = 1,
        )
      assert(res.size == 2)
      assert(res[0].id == event2.id)
    }

    @Test
    fun `should list minimal representation of one connection`() {
      val res =
        connectionTimelineEventRepository.findByConnectionIdsMinimal(
          connectionIds = listOf(connectionId),
          eventTypes = listOf(ConnectionEvent.Type.REFRESH_SUCCEEDED),
          createdAtStart = OffsetDateTime.of(2024, 9, 2, 0, 0, 0, 0, ZoneOffset.UTC),
          createdAtEnd = OffsetDateTime.of(2024, 9, 5, 0, 0, 0, 0, ZoneOffset.UTC),
        )
      assert(res.size == 1)
    }

    @Test
    fun `should list minimal representation of two connections`() {
      val res =
        connectionTimelineEventRepository.findByConnectionIdsMinimal(
          connectionIds = listOf(connectionId, secondConnectionId),
          eventTypes = listOf(ConnectionEvent.Type.REFRESH_STARTED, ConnectionEvent.Type.REFRESH_SUCCEEDED),
          createdAtStart = OffsetDateTime.of(2024, 9, 2, 0, 0, 0, 0, ZoneOffset.UTC),
          createdAtEnd = OffsetDateTime.of(2024, 9, 5, 0, 0, 0, 0, ZoneOffset.UTC),
        )
      assert(res.size == 3)
    }

    @Test
    fun `should list minimal representation of two connections and multiple event types`() {
      val res =
        connectionTimelineEventRepository.findByConnectionIdsMinimal(
          connectionIds = listOf(connectionId, secondConnectionId),
          eventTypes =
            listOf(
              ConnectionEvent.Type.SYNC_SUCCEEDED,
              ConnectionEvent.Type.REFRESH_SUCCEEDED,
              ConnectionEvent.Type.REFRESH_STARTED,
              ConnectionEvent.Type.SYNC_STARTED,
              ConnectionEvent.Type.SYNC_CANCELLED,
            ),
          createdAtStart = OffsetDateTime.of(2023, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC),
          createdAtEnd = OffsetDateTime.of(2024, 12, 31, 0, 0, 0, 0, ZoneOffset.UTC),
        )
      assert(res.size == 6)
    }

    @Test
    fun `should list minimal representation when filtering by createdAtStart`() {
      val res =
        connectionTimelineEventRepository.findByConnectionIdsMinimal(
          connectionIds = listOf(connectionId, secondConnectionId),
          eventTypes =
            listOf(
              ConnectionEvent.Type.SYNC_SUCCEEDED,
              ConnectionEvent.Type.REFRESH_SUCCEEDED,
              ConnectionEvent.Type.REFRESH_STARTED,
              ConnectionEvent.Type.SYNC_STARTED,
              ConnectionEvent.Type.SYNC_CANCELLED,
            ),
          createdAtStart = OffsetDateTime.of(2024, 9, 4, 0, 0, 0, 0, ZoneOffset.UTC),
          createdAtEnd = OffsetDateTime.of(2024, 12, 31, 0, 0, 0, 0, ZoneOffset.UTC),
        )
      assert(res.size == 2)
    }
  }

  @Nested
  inner class FindAssociatedUserForJobTest {
    private val connectionId: UUID = UUID.randomUUID()
    private val userId1: UUID = UUID.randomUUID()
    private val userId2: UUID = UUID.randomUUID()
    private val jobId = 6269L
    private val event1 =
      ConnectionTimelineEvent(
        connectionId = connectionId,
        eventType = ConnectionEvent.Type.REFRESH_STARTED.name,
        createdAt = OffsetDateTime.of(2024, 10, 1, 0, 0, 0, 1, ZoneOffset.UTC),
        summary = """
          {"jobId": 6269, "streams": [{"name": "users", "namespace": "public"}]}
        """,
        userId = userId1,
      )

    // In case we have duped events created in the DB (which we have seen this issue sometimes),
    // I am mocking another duped event having the same connectionId, jobId and userId. The timestamp is a little bit later than event1.
    // And we should expect to get the userId from the first event (event1) as the result.
    private val event2 =
      ConnectionTimelineEvent(
        connectionId = connectionId,
        eventType = ConnectionEvent.Type.REFRESH_STARTED.name,
        createdAt = OffsetDateTime.of(2024, 10, 1, 0, 0, 0, 0, ZoneOffset.UTC),
        userId = userId2,
        summary = """
          {"jobId": 6269, "streams": [{"name": "users", "namespace": "public"}]}
        """,
      )
    private val event3 =
      ConnectionTimelineEvent(
        connectionId = connectionId,
        eventType = ConnectionEvent.Type.REFRESH_CANCELLED.name,
        createdAt = OffsetDateTime.of(2024, 9, 2, 0, 0, 0, 0, ZoneOffset.UTC),
        summary = """
          {"jobId": 6269, "streams": [{"name": "users", "namespace": "public"}], "bytesLoaded": 0, "attemptsCount": 1, "recordsLoaded": 0, "endTimeEpochSeconds": 1721362846}
        """,
        userId = null,
      )
    private val event4 =
      ConnectionTimelineEvent(
        connectionId = connectionId,
        eventType = ConnectionEvent.Type.SYNC_STARTED.name,
        createdAt = OffsetDateTime.of(2024, 9, 4, 0, 0, 0, 0, ZoneOffset.UTC),
      )

    @BeforeEach
    fun setup() {
      // save some events
      val allEvents = listOf(event1, event2, event3, event4)
      allEvents.forEach { event -> connectionTimelineEventRepository.save(event) }
    }

    @AfterEach
    fun reset() {
      connectionTimelineEventRepository.deleteAll()
    }

    @Test
    fun `should get the userId from the event`() {
      val res =
        connectionTimelineEventRepository.findAssociatedUserForAJob(
          connectionId = connectionId,
          eventType = ConnectionEvent.Type.REFRESH_STARTED,
          createdAtStart = event3.createdAt,
          jobId = jobId,
        )
      assert(res.size == 2)
      assert(res[0] == userId2)
    }

    @Test
    fun `should get null because there are no events`() {
      val res =
        connectionTimelineEventRepository.findAssociatedUserForAJob(
          connectionId = connectionId,
          eventType = ConnectionEvent.Type.SYNC_STARTED,
          createdAtStart = event1.createdAt,
          jobId = jobId,
        )
      assert(res.isEmpty())
    }

    @Test
    fun `should get null because the user is null in the event`() {
      val res =
        connectionTimelineEventRepository.findAssociatedUserForAJob(
          connectionId = connectionId,
          eventType = ConnectionEvent.Type.REFRESH_CANCELLED,
          createdAtStart = event3.createdAt,
          jobId = jobId,
        )
      assert(res.size == 1)
      assert(res[0] == null)
    }
  }

  @Nested
  inner class FindByJobIdTest {
    private val connectionId: UUID = UUID.randomUUID()
    private val jobId1 = 1234L
    private val jobId2 = 5678L
    private val event1 =
      ConnectionTimelineEvent(
        connectionId = connectionId,
        eventType = ConnectionEvent.Type.SYNC_STARTED.name,
        createdAt = OffsetDateTime.of(2024, 9, 1, 0, 0, 0, 0, ZoneOffset.UTC),
        jobId = jobId1,
      )
    private val event2 =
      ConnectionTimelineEvent(
        connectionId = connectionId,
        eventType = ConnectionEvent.Type.SYNC_SUCCEEDED.name,
        createdAt = OffsetDateTime.of(2024, 9, 2, 0, 0, 0, 0, ZoneOffset.UTC),
        jobId = jobId1,
      )
    private val event3 =
      ConnectionTimelineEvent(
        connectionId = connectionId,
        eventType = ConnectionEvent.Type.REFRESH_STARTED.name,
        createdAt = OffsetDateTime.of(2024, 9, 3, 0, 0, 0, 0, ZoneOffset.UTC),
        jobId = jobId2,
      )
    private val event4 =
      ConnectionTimelineEvent(
        connectionId = connectionId,
        eventType = ConnectionEvent.Type.REFRESH_SUCCEEDED.name,
        createdAt = OffsetDateTime.of(2024, 9, 4, 0, 0, 0, 0, ZoneOffset.UTC),
        jobId = null,
      )

    @BeforeEach
    fun setup() {
      val allEvents = listOf(event1, event2, event3, event4)
      allEvents.forEach { event -> connectionTimelineEventRepository.save(event) }
    }

    @AfterEach
    fun reset() {
      connectionTimelineEventRepository.deleteAll()
    }

    @Test
    fun `should find events by jobId`() {
      val res = connectionTimelineEventRepository.findByJobId(jobId1)
      assert(res.size == 2)
      val eventIds = res.map { it.id }
      assert(eventIds.contains(event1.id))
      assert(eventIds.contains(event2.id))
    }

    @Test
    fun `should find single event by jobId`() {
      val res = connectionTimelineEventRepository.findByJobId(jobId2)
      assert(res.size == 1)
      assert(res[0].id == event3.id)
    }

    @Test
    fun `should return empty list for non-existent jobId`() {
      val res = connectionTimelineEventRepository.findByJobId(9999L)
      assert(res.isEmpty())
    }
  }
}
