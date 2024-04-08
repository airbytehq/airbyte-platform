package io.airbyte.config.persistence

import io.airbyte.config.persistence.domain.Generation
import io.airbyte.config.persistence.domain.StreamGeneration
import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

@MicronautTest(environments = [Environment.TEST])
class StreamGenerationRepositoryTest : RepositoryTestSetup() {
  @AfterEach
  fun cleanDb() {
    getRepository(StreamGenerationRepository::class.java).deleteAll()
  }

  @Test
  fun `test db insertion`() {
    val streamGeneration =
      StreamGeneration(
        connectionId = connectionId1,
        streamName = "sname",
        streamNamespace = "snamespace",
        generationId = 0,
        startJobId = 0,
      )

    getRepository(StreamGenerationRepository::class.java).save(streamGeneration)

    assertEquals(1, getRepository(StreamGenerationRepository::class.java).findByConnectionId(streamGeneration.connectionId).size)
  }

  @Test
  fun `find by connection id and stream name`() {
    val streamGeneration =
      StreamGeneration(
        connectionId = connectionId1,
        streamName = "sname1",
        streamNamespace = "snamespace1",
        generationId = 0,
        startJobId = 0,
      )

    getRepository(StreamGenerationRepository::class.java).save(streamGeneration)

    val streamGeneration2 =
      StreamGeneration(
        connectionId = connectionId1,
        streamName = "sname2",
        streamNamespace = "snamespace2",
        generationId = 1,
        startJobId = 1,
      )

    getRepository(StreamGenerationRepository::class.java).save(streamGeneration2)

    val streamGeneration3 =
      StreamGeneration(
        connectionId = connectionId2,
        streamName = "sname3",
        generationId = 2,
        startJobId = 2,
      )

    getRepository(StreamGenerationRepository::class.java).save(streamGeneration3)

    val streamGenerationForConnectionIds = getRepository(StreamGenerationRepository::class.java).findByConnectionId(connectionId1)
    assertEquals(2, streamGenerationForConnectionIds.size)

    val maxGenerationOfStreamsByConnectionId1 =
      getRepository(
        StreamGenerationRepository::class.java,
      ).getMaxGenerationOfStreamsForConnectionId(connectionId1)
    val expectedRecord1 = Generation("sname1", "snamespace1", 0)
    val expectedRecord2 = Generation("sname2", "snamespace2", 1)
    assertEquals(2, maxGenerationOfStreamsByConnectionId1.size)
    assertThat(maxGenerationOfStreamsByConnectionId1).containsExactlyInAnyOrder(expectedRecord1, expectedRecord2)

    val maxGenerationOfStreamsByConnectionId2 =
      getRepository(
        StreamGenerationRepository::class.java,
      ).getMaxGenerationOfStreamsForConnectionId(connectionId2)
    assertEquals(1, maxGenerationOfStreamsByConnectionId2.size)
    val expectedRecord3 = Generation(streamName = "sname3", generationId = 2)
    assertThat(maxGenerationOfStreamsByConnectionId2).containsExactlyInAnyOrder(expectedRecord3)
  }

  @Test
  fun `delete by connection id`() {
    val streamGeneration =
      StreamGeneration(
        connectionId = connectionId1,
        streamName = "sname1",
        streamNamespace = "snamespace1",
        generationId = 0,
        startJobId = 0,
      )

    getRepository(StreamGenerationRepository::class.java).save(streamGeneration)

    val streamGeneration2 =
      StreamGeneration(
        connectionId = connectionId2,
        streamName = "sname2",
        streamNamespace = "sname2",
        generationId = 1,
        startJobId = 1,
      )

    getRepository(StreamGenerationRepository::class.java).save(streamGeneration2)

    getRepository(StreamGenerationRepository::class.java).deleteByConnectionId(streamGeneration.connectionId)

    assertTrue(getRepository(StreamGenerationRepository::class.java).findByConnectionId(streamGeneration.connectionId).isEmpty())
    assertTrue(getRepository(StreamGenerationRepository::class.java).findByConnectionId(streamGeneration2.connectionId).isNotEmpty())
  }
}
