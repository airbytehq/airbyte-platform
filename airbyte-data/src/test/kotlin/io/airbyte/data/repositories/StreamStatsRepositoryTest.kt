package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.StreamStats
import io.airbyte.db.instance.jobs.jooq.generated.Keys
import io.airbyte.db.instance.jobs.jooq.generated.Tables
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.util.UUID

@MicronautTest
internal class StreamStatsRepositoryTest : AbstractConfigRepositoryTest() {
  companion object {
    private const val ATTEMPT_1 = 1L
    private const val ATTEMPT_2 = 2L

    private val connectionId: UUID = UUID.randomUUID()
    private const val STREAM_NAME_FOO = "foo"
    private const val STREAM_NAME_BAR = "bar"
    private const val STREAM_NAMESPACE_1 = "ns1"

    private val streamStats1 =
      StreamStats(
        attemptId = ATTEMPT_1,
        streamName = STREAM_NAME_FOO,
        streamNamespace = STREAM_NAMESPACE_1,
        recordsEmitted = 10,
        bytesEmitted = 100,
        connectionId = connectionId,
      )

    private val streamStats2 =
      StreamStats(
        attemptId = ATTEMPT_2,
        streamName = STREAM_NAME_BAR,
        streamNamespace = STREAM_NAMESPACE_1,
        recordsEmitted = 20,
        bytesEmitted = 200,
        connectionId = connectionId,
      )

    @BeforeAll
    @JvmStatic
    fun setup() {
      // so we don't have to deal with making attempts as well
      jooqDslContext
        .alterTable(
          Tables.STREAM_STATS,
        ).dropForeignKey(Keys.STREAM_STATS__STREAM_STATS_ATTEMPT_ID_FKEY.constraint())
        .execute()
    }
  }

  @Test
  fun `test basic db insertion and retrieval`() {
    val saveResult1 = streamStatsRepository.save(streamStats1)
    val saveResult2 = streamStatsRepository.save(streamStats2)

    val actualStreamStats1 = streamStatsRepository.findById(saveResult1.id!!).get()
    val actualStreamStats2 = streamStatsRepository.findById(saveResult2.id!!).get()

    assertThat(actualStreamStats1)
      .usingRecursiveComparison()
      .ignoringFields("id", "createdAt", "updatedAt")
      .isEqualTo(streamStats1)

    assertThat(actualStreamStats2)
      .usingRecursiveComparison()
      .ignoringFields("id", "createdAt", "updatedAt")
      .isEqualTo(streamStats2)
  }
}
