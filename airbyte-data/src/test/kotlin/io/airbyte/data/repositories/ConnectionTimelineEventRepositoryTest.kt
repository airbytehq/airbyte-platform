package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.ConnectionTimelineEvent
import io.airbyte.db.instance.configs.jooq.generated.Keys
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.util.UUID

@MicronautTest
internal class ConnectionTimelineEventRepositoryTest : AbstractConfigRepositoryTest<ConnectionTimelineEventRepository>(
  ConnectionTimelineEventRepository::class,
) {
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
    val eventId = java.util.UUID.randomUUID()
    val event =
      ConnectionTimelineEvent(
        connectionId = UUID.randomUUID(),
        eventType = "Test",
      )

    val saved = repository.save(event)
    assert(repository.count() == 1L)

    val persistedEvent = repository.findById(saved.id!!).get()
    assert(persistedEvent.connectionId == event.connectionId)
  }
}
