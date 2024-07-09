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
import java.util.UUID

@MicronautTest
internal class ConnectionTimelineEventRepositoryTest : AbstractConfigRepositoryTest() {
  companion object {
    @BeforeAll
    @JvmStatic
    fun setup() {
      // so we don't have to deal with making users as well
      jooqDslContext.alterTable(
        Tables.CONNECTION_TIMELINE_EVENT,
      ).dropForeignKey(Keys.CONNECTION_TIMELINE_EVENT__CONNECTION_TIMELINE_EVENT_CONNECTION_ID_FKEY.constraint()).execute()
    }
  }

  @Test
  fun `test db insertion`() {
    val event =
      ConnectionTimelineEvent(
        connectionId = UUID.randomUUID(),
        eventType = "Test",
      )

    val saved = connectionTimelineEventRepository.save(event)
    assert(connectionTimelineEventRepository.count() == 1L)

    val persistedEvent = connectionTimelineEventRepository.findById(saved.id!!).get()
    assert(persistedEvent.connectionId == event.connectionId)
  }

  @Nested
  inner class ListEventsTest {
    private val connectionId: UUID = UUID.randomUUID()
    private val event1 =
      ConnectionTimelineEvent(
        connectionId = connectionId,
        eventType = ConnectionEvent.Type.SYNC_STARTED.name,
      )
    private val event2 =
      ConnectionTimelineEvent(
        connectionId = connectionId,
        eventType = ConnectionEvent.Type.SYNC_CANCELLED.name,
      )
    private val event3 =
      ConnectionTimelineEvent(
        connectionId = connectionId,
        eventType = ConnectionEvent.Type.REFRESH_STARTED.name,
      )
    private val event4 =
      ConnectionTimelineEvent(
        connectionId = connectionId,
        eventType = ConnectionEvent.Type.REFRESH_SUCCEEDED.name,
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
      assert(connectionTimelineEventRepository.count() == 4L)
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
  }
}
