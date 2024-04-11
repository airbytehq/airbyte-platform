package io.airbyte.config.persistence

import io.airbyte.config.persistence.domain.StreamRefresh
import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

@MicronautTest(environments = [Environment.TEST])
class StreamRefreshesRepositoryTest : RepositoryTestSetup() {
  @AfterEach
  fun cleanDb() {
    getRepository(StreamRefreshesRepository::class.java).deleteAll()
  }

  @Test
  fun `test db insertion`() {
    val streamRefresh =
      StreamRefresh(
        connectionId = connectionId1,
        streamName = "sname",
        streamNamespace = "snamespace",
      )

    getRepository(StreamRefreshesRepository::class.java).save(streamRefresh)
    assertTrue(getRepository(StreamRefreshesRepository::class.java).existsByConnectionId(streamRefresh.connectionId))
  }

  @Test
  fun `find by connection id`() {
    val streamRefresh1 =
      StreamRefresh(
        connectionId = connectionId1,
        streamName = "sname1",
        streamNamespace = "snamespace1",
      )

    getRepository(StreamRefreshesRepository::class.java).save(streamRefresh1)

    val streamRefresh2 =
      StreamRefresh(
        connectionId = connectionId1,
        streamName = "sname2",
        streamNamespace = "snamespace2",
      )

    getRepository(StreamRefreshesRepository::class.java).save(streamRefresh2)

    val streamRefresh3 =
      StreamRefresh(
        connectionId = connectionId2,
        streamName = "sname3",
        streamNamespace = "snamespace3",
      )

    getRepository(StreamRefreshesRepository::class.java).save(streamRefresh3)

    assertEquals(2, getRepository(StreamRefreshesRepository::class.java).findByConnectionId(connectionId1).size)
  }

  @Test
  fun `delete by connection id`() {
    val streamRefresh1 =
      StreamRefresh(
        connectionId = connectionId1,
        streamName = "sname1",
        streamNamespace = "snamespace1",
      )

    getRepository(StreamRefreshesRepository::class.java).save(streamRefresh1)

    val streamRefresh2 =
      StreamRefresh(
        connectionId = connectionId2,
        streamName = "sname2",
        streamNamespace = "snamespace2",
      )

    getRepository(StreamRefreshesRepository::class.java).save(streamRefresh2)

    getRepository(StreamRefreshesRepository::class.java).deleteByConnectionId(streamRefresh1.connectionId)

    assertTrue(getRepository(StreamRefreshesRepository::class.java).findByConnectionId(streamRefresh1.connectionId).isEmpty())
    assertTrue(getRepository(StreamRefreshesRepository::class.java).findByConnectionId(streamRefresh2.connectionId).isNotEmpty())
  }

  @Test
  fun `delete by connection id and stream name and namespace`() {
    val streamRefresh1 =
      StreamRefresh(
        connectionId = connectionId1,
        streamName = "sname1",
        streamNamespace = "snamespace1",
      )

    val streamRefresh2 =
      StreamRefresh(
        connectionId = connectionId1,
        streamName = "sname2",
        streamNamespace = "snamespace2",
      )

    val streamRefresh3 =
      StreamRefresh(
        connectionId = connectionId1,
        streamName = "sname3",
      )

    getRepository(StreamRefreshesRepository::class.java).saveAll(listOf(streamRefresh1, streamRefresh2, streamRefresh3))

    getRepository(
      StreamRefreshesRepository::class.java,
    ).deleteByConnectionIdAndStreamNameAndStreamNamespace(connectionId1, streamRefresh3.streamName, streamRefresh3.streamNamespace)
    val refreshes: List<StreamRefresh> = getRepository(StreamRefreshesRepository::class.java).findByConnectionId(connectionId1)
    assertEquals(2, refreshes.size)
    refreshes.forEach {
      assertEquals(connectionId1, it.connectionId)
      if (streamRefresh1.streamName.equals(it.streamName)) {
        assertEquals(streamRefresh1.streamNamespace, it.streamNamespace)
      } else if (streamRefresh2.streamName.equals(it.streamName)) {
        assertEquals(streamRefresh2.streamNamespace, it.streamNamespace)
      } else {
        throw RuntimeException("Unknown stream name " + it.streamName)
      }
    }

    getRepository(
      StreamRefreshesRepository::class.java,
    ).deleteByConnectionIdAndStreamNameAndStreamNamespace(connectionId1, streamRefresh2.streamName, streamRefresh2.streamNamespace)
    val refreshes2: List<StreamRefresh> = getRepository(StreamRefreshesRepository::class.java).findByConnectionId(connectionId1)
    assertEquals(1, refreshes2.size)
    refreshes2.forEach {
      assertEquals(connectionId1, it.connectionId)
      assertEquals(streamRefresh1.streamName, (it.streamName))
      assertEquals(streamRefresh1.streamNamespace, it.streamNamespace)
    }
  }
}
