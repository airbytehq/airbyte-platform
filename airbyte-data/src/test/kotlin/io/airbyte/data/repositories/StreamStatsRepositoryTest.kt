/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.data.repositories.entities.StreamStats
import io.airbyte.db.instance.jobs.jooq.generated.Keys
import io.airbyte.db.instance.jobs.jooq.generated.Tables
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.assertj.core.api.Assertions.assertThat
import org.jooq.JSONB
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.util.UUID

@MicronautTest
internal class StreamStatsRepositoryTest : AbstractConfigRepositoryTest() {
  companion object {
    private val connectionId: UUID = UUID.randomUUID()
    private const val STREAM_NAMESPACE_1 = "ns1"

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
  fun `test findByAttemptId`() {
    val additionalStatsMap1 = mapOf("metric1" to 1.5.toBigDecimal(), "metric2" to 2.5.toBigDecimal())
    val additionalStatsMap2 = mapOf("metric1" to 3.5.toBigDecimal(), "metric2" to 4.5.toBigDecimal())

    val objectMapper = ObjectMapper()
    val additionalStatsJson1 = JSONB.valueOf(objectMapper.writeValueAsString(additionalStatsMap1))
    val additionalStatsJson2 = JSONB.valueOf(objectMapper.writeValueAsString(additionalStatsMap2))
    val additionalStatsJson3 = JSONB.valueOf(objectMapper.writeValueAsString(mapOf("metric1" to 5.5.toBigDecimal())))

    val id1 = UUID.randomUUID()
    val id2 = UUID.randomUUID()
    val id3 = UUID.randomUUID()
    val metadataId1 = UUID.randomUUID()
    val metadataId2 = UUID.randomUUID()
    val now = java.time.OffsetDateTime.now()

    val streamNameBaz = "baz"
    val streamNameQux = "qux"
    val streamNameQuux = "quux"
    val attemptIdForTest = 2000L
    val attemptIdForTest2 = 2001L
    val jobIdForTest = 2000L

    // Insert dummy attempts for foreign key constraint
    jooqDslContext
      .insertInto(Tables.ATTEMPTS)
      .columns(Tables.ATTEMPTS.ID, Tables.ATTEMPTS.JOB_ID, Tables.ATTEMPTS.CREATED_AT, Tables.ATTEMPTS.UPDATED_AT)
      .values(attemptIdForTest, jobIdForTest, now, now)
      .values(attemptIdForTest2, jobIdForTest, now, now)
      .execute()

    // Insert stream stats with additional_stats using JOOQ
    jooqDslContext
      .insertInto(Tables.STREAM_STATS)
      .columns(
        Tables.STREAM_STATS.ID,
        Tables.STREAM_STATS.ATTEMPT_ID,
        Tables.STREAM_STATS.STREAM_NAME,
        Tables.STREAM_STATS.STREAM_NAMESPACE,
        Tables.STREAM_STATS.CONNECTION_ID,
        Tables.STREAM_STATS.ADDITIONAL_STATS,
        Tables.STREAM_STATS.CREATED_AT,
        Tables.STREAM_STATS.UPDATED_AT,
      ).values(id1, attemptIdForTest, streamNameBaz, STREAM_NAMESPACE_1, connectionId, additionalStatsJson1, now, now)
      .values(id2, attemptIdForTest, streamNameQux, STREAM_NAMESPACE_1, connectionId, additionalStatsJson2, now, now)
      .values(id3, attemptIdForTest2, streamNameQuux, STREAM_NAMESPACE_1, connectionId, additionalStatsJson3, now, now)
      .execute()

    // Insert stream attempt metadata
    jooqDslContext
      .insertInto(Tables.STREAM_ATTEMPT_METADATA)
      .columns(
        Tables.STREAM_ATTEMPT_METADATA.ID,
        Tables.STREAM_ATTEMPT_METADATA.ATTEMPT_ID,
        Tables.STREAM_ATTEMPT_METADATA.STREAM_NAME,
        Tables.STREAM_ATTEMPT_METADATA.STREAM_NAMESPACE,
        Tables.STREAM_ATTEMPT_METADATA.WAS_BACKFILLED,
        Tables.STREAM_ATTEMPT_METADATA.WAS_RESUMED,
      ).values(metadataId1, attemptIdForTest, streamNameBaz, STREAM_NAMESPACE_1, true, false)
      .values(metadataId2, attemptIdForTest, streamNameQux, STREAM_NAMESPACE_1, false, true)
      .execute()

    val results = streamStatsRepository.findByAttemptId(attemptIdForTest)

    assertThat(results).hasSize(2)

    val result1 = results.find { it.id == id1 }
    assertThat(result1).isNotNull
    assertThat(result1!!.attemptId).isEqualTo(attemptIdForTest)
    assertThat(result1.streamName).isEqualTo(streamNameBaz)
    assertThat(result1.streamNamespace).isEqualTo(STREAM_NAMESPACE_1)
    assertThat(result1.additionalStats).isEqualTo(additionalStatsMap1)
    assertThat(result1.wasBackfilled).isEqualTo(true)
    assertThat(result1.wasResumed).isEqualTo(false)

    val result2 = results.find { it.id == id2 }
    assertThat(result2).isNotNull
    assertThat(result2!!.attemptId).isEqualTo(attemptIdForTest)
    assertThat(result2.streamName).isEqualTo(streamNameQux)
    assertThat(result2.streamNamespace).isEqualTo(STREAM_NAMESPACE_1)
    assertThat(result2.additionalStats).isEqualTo(additionalStatsMap2)
    assertThat(result2.wasBackfilled).isEqualTo(false)
    assertThat(result2.wasResumed).isEqualTo(true)
  }

  @Test
  fun `test findByJobId`() {
    val additionalStatsMap1 = mapOf("metric1" to 10.0.toBigDecimal(), "metric2" to 20.0.toBigDecimal())
    val additionalStatsMap2 = mapOf("metric1" to 30.0.toBigDecimal(), "metric2" to 40.0.toBigDecimal())
    val additionalStatsMap3 = mapOf("metric1" to 50.0.toBigDecimal(), "metric2" to 60.0.toBigDecimal())

    val objectMapper = ObjectMapper()
    val additionalStatsJson1 = JSONB.valueOf(objectMapper.writeValueAsString(additionalStatsMap1))
    val additionalStatsJson2 = JSONB.valueOf(objectMapper.writeValueAsString(additionalStatsMap2))
    val additionalStatsJson3 = JSONB.valueOf(objectMapper.writeValueAsString(additionalStatsMap3))

    val attempt1Id = 3000L
    val attempt2Id = 3001L
    val attempt3Id = 3002L
    val job1 = 3000L
    val job2 = 3001L

    val streamStatsId1 = UUID.randomUUID()
    val streamStatsId2 = UUID.randomUUID()
    val streamStatsId3 = UUID.randomUUID()

    val metadataId1 = UUID.randomUUID()
    val metadataId2 = UUID.randomUUID()
    val metadataId3 = UUID.randomUUID()

    val now = java.time.OffsetDateTime.now()

    // Insert attempts for job1 and job2
    jooqDslContext
      .insertInto(Tables.ATTEMPTS)
      .columns(Tables.ATTEMPTS.ID, Tables.ATTEMPTS.JOB_ID, Tables.ATTEMPTS.CREATED_AT, Tables.ATTEMPTS.UPDATED_AT)
      .values(attempt1Id, job1, now, now)
      .values(attempt2Id, job1, now, now)
      .values(attempt3Id, job2, now, now)
      .execute()

    // Insert stream stats for these attempts
    jooqDslContext
      .insertInto(Tables.STREAM_STATS)
      .columns(
        Tables.STREAM_STATS.ID,
        Tables.STREAM_STATS.ATTEMPT_ID,
        Tables.STREAM_STATS.STREAM_NAME,
        Tables.STREAM_STATS.STREAM_NAMESPACE,
        Tables.STREAM_STATS.CONNECTION_ID,
        Tables.STREAM_STATS.ADDITIONAL_STATS,
        Tables.STREAM_STATS.CREATED_AT,
        Tables.STREAM_STATS.UPDATED_AT,
      ).values(streamStatsId1, attempt1Id, "stream1", STREAM_NAMESPACE_1, connectionId, additionalStatsJson1, now, now)
      .values(streamStatsId2, attempt2Id, "stream2", STREAM_NAMESPACE_1, connectionId, additionalStatsJson2, now, now)
      .values(streamStatsId3, attempt3Id, "stream3", STREAM_NAMESPACE_1, connectionId, additionalStatsJson3, now, now)
      .execute()

    // Insert stream attempt metadata
    jooqDslContext
      .insertInto(Tables.STREAM_ATTEMPT_METADATA)
      .columns(
        Tables.STREAM_ATTEMPT_METADATA.ID,
        Tables.STREAM_ATTEMPT_METADATA.ATTEMPT_ID,
        Tables.STREAM_ATTEMPT_METADATA.STREAM_NAME,
        Tables.STREAM_ATTEMPT_METADATA.STREAM_NAMESPACE,
        Tables.STREAM_ATTEMPT_METADATA.WAS_BACKFILLED,
        Tables.STREAM_ATTEMPT_METADATA.WAS_RESUMED,
      ).values(metadataId1, attempt1Id, "stream1", STREAM_NAMESPACE_1, false, false)
      .values(metadataId2, attempt2Id, "stream2", STREAM_NAMESPACE_1, true, true)
      .values(metadataId3, attempt3Id, "stream3", STREAM_NAMESPACE_1, false, false)
      .execute()

    val results = streamStatsRepository.findByJobId(job1)

    assertThat(results).hasSize(2)

    val result1 = results.find { it.id == streamStatsId1 }
    assertThat(result1).isNotNull
    assertThat(result1!!.attemptId).isEqualTo(attempt1Id)
    assertThat(result1.streamName).isEqualTo("stream1")
    assertThat(result1.streamNamespace).isEqualTo(STREAM_NAMESPACE_1)
    assertThat(result1.additionalStats).isEqualTo(additionalStatsMap1)
    assertThat(result1.wasBackfilled).isEqualTo(false)
    assertThat(result1.wasResumed).isEqualTo(false)

    val result2 = results.find { it.id == streamStatsId2 }
    assertThat(result2).isNotNull
    assertThat(result2!!.attemptId).isEqualTo(attempt2Id)
    assertThat(result2.streamName).isEqualTo("stream2")
    assertThat(result2.streamNamespace).isEqualTo(STREAM_NAMESPACE_1)
    assertThat(result2.additionalStats).isEqualTo(additionalStatsMap2)
    assertThat(result2.wasBackfilled).isEqualTo(true)
    assertThat(result2.wasResumed).isEqualTo(true)

    // Verify that stream stats from job2 are not included
    val result3 = results.find { it.id == streamStatsId3 }
    assertThat(result3).isNull()
  }
}
