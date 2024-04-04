package io.airbyte.config.persistence

import io.airbyte.config.persistence.domain.StreamRefresh
import io.airbyte.config.persistence.domain.StreamRefreshPK
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
        StreamRefreshPK(
          connectionId = connectionId1,
          streamName = "sname",
          streamNamespace = "snamespace",
        ),
      )

    getRepository(StreamRefreshesRepository::class.java).save(streamRefresh)

    assertTrue(getRepository(StreamRefreshesRepository::class.java).existsById(streamRefresh.pk))
  }

  @Test
  fun `find by connection id`() {
    val streamRefresh1 =
      StreamRefresh(
        StreamRefreshPK(
          connectionId = connectionId1,
          streamName = "sname1",
          streamNamespace = "snamespace1",
        ),
      )

    getRepository(StreamRefreshesRepository::class.java).save(streamRefresh1)

    val streamRefresh2 =
      StreamRefresh(
        StreamRefreshPK(
          connectionId = connectionId1,
          streamName = "sname2",
          streamNamespace = "snamespace2",
        ),
      )

    getRepository(StreamRefreshesRepository::class.java).save(streamRefresh2)

    val streamRefresh3 =
      StreamRefresh(
        StreamRefreshPK(
          connectionId = connectionId2,
          streamName = "sname3",
          streamNamespace = "snamespace3",
        ),
      )

    getRepository(StreamRefreshesRepository::class.java).save(streamRefresh3)

    assertEquals(2, getRepository(StreamRefreshesRepository::class.java).findByPkConnectionId(connectionId1).size)
  }

  @Test
  fun `delete by connection id`() {
    val streamRefresh1 =
      StreamRefresh(
        StreamRefreshPK(
          connectionId = connectionId1,
          streamName = "sname1",
          streamNamespace = "snamespace1",
        ),
      )

    getRepository(StreamRefreshesRepository::class.java).save(streamRefresh1)

    val streamRefresh2 =
      StreamRefresh(
        StreamRefreshPK(
          connectionId = connectionId2,
          streamName = "sname2",
          streamNamespace = "snamespace2",
        ),
      )

    getRepository(StreamRefreshesRepository::class.java).save(streamRefresh2)

    getRepository(StreamRefreshesRepository::class.java).deleteByPkConnectionId(streamRefresh1.pk.connectionId)

    assertTrue(getRepository(StreamRefreshesRepository::class.java).findById(streamRefresh1.pk).isEmpty)
    assertTrue(getRepository(StreamRefreshesRepository::class.java).findById(streamRefresh2.pk).isPresent)
  }
}
