/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.DataplaneHeartbeatLog
import io.airbyte.db.instance.configs.jooq.generated.Keys
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

@MicronautTest
class DataplaneHeartbeatLogRepositoryTest : AbstractConfigRepositoryTest() {
  companion object {
    @BeforeAll
    @JvmStatic
    fun setup() {
      // so we don't have to deal with making a dataplane
      jooqDslContext
        .alterTable(
          Tables.DATAPLANE_HEARTBEAT_LOG,
        ).dropForeignKey(Keys.DATAPLANE_HEARTBEAT_LOG__DATAPLANE_HEARTBEAT_LOG_DATAPLANE_ID_FKEY.constraint())
        .execute()
    }
  }

  @AfterEach
  fun cleanup() {
    jooqDslContext
      .deleteFrom(Tables.DATAPLANE_HEARTBEAT_LOG)
      .execute()
  }

  @Test
  fun `create and retrieve dataplane heartbeat log`() {
    val dataplaneId = UUID.randomUUID()
    val heartbeatLog =
      DataplaneHeartbeatLog(
        dataplaneId = dataplaneId,
        controlPlaneVersion = "1.0.0",
        dataplaneVersion = "2.0.0",
      )

    val saved = dataplaneHeartbeatLogRepository.save(heartbeatLog)
    assertThat(saved.id).isNotNull()
    assertThat(saved.createdAt).isNotNull()
    assertEquals(dataplaneId, saved.dataplaneId)
    assertEquals("1.0.0", saved.controlPlaneVersion)
    assertEquals("2.0.0", saved.dataplaneVersion)
  }

  @Test
  fun `findLatestHeartbeatsByDataplaneIds with single dataplane`() {
    val dataplaneId = UUID.randomUUID()

    // Create multiple heartbeats with different timestamps
    val log1 =
      dataplaneHeartbeatLogRepository.save(
        DataplaneHeartbeatLog(
          dataplaneId = dataplaneId,
          controlPlaneVersion = "1.0.0",
          dataplaneVersion = "2.0.0",
        ),
      )

    Thread.sleep(10) // Ensure different timestamps

    val log2 =
      dataplaneHeartbeatLogRepository.save(
        DataplaneHeartbeatLog(
          dataplaneId = dataplaneId,
          controlPlaneVersion = "1.0.1",
          dataplaneVersion = "2.0.1",
        ),
      )

    val latest = dataplaneHeartbeatLogRepository.findLatestHeartbeatsByDataplaneIds(listOf(dataplaneId))

    assertEquals(1, latest.size)
    assertEquals(log2.id, latest[0].id)
    assertEquals("1.0.1", latest[0].controlPlaneVersion)
    assertEquals("2.0.1", latest[0].dataplaneVersion)
  }

  @Test
  fun `findLatestHeartbeatsByDataplaneIds with multiple dataplanes`() {
    val dataplaneId1 = UUID.randomUUID()
    val dataplaneId2 = UUID.randomUUID()

    // Create heartbeats for first dataplane
    dataplaneHeartbeatLogRepository.save(
      DataplaneHeartbeatLog(
        dataplaneId = dataplaneId1,
        controlPlaneVersion = "1.0.0",
        dataplaneVersion = "2.0.0",
      ),
    )

    Thread.sleep(10)

    val log1Latest =
      dataplaneHeartbeatLogRepository.save(
        DataplaneHeartbeatLog(
          dataplaneId = dataplaneId1,
          controlPlaneVersion = "1.0.1",
          dataplaneVersion = "2.0.1",
        ),
      )

    // Create heartbeats for second dataplane
    val log2Latest =
      dataplaneHeartbeatLogRepository.save(
        DataplaneHeartbeatLog(
          dataplaneId = dataplaneId2,
          controlPlaneVersion = "1.1.0",
          dataplaneVersion = "2.1.0",
        ),
      )

    val latest = dataplaneHeartbeatLogRepository.findLatestHeartbeatsByDataplaneIds(listOf(dataplaneId1, dataplaneId2))

    assertEquals(2, latest.size)
    val latestById = latest.associateBy { it.dataplaneId }

    assertThat(latestById[dataplaneId1]!!.id).isEqualTo(log1Latest.id)
    assertThat(latestById[dataplaneId2]!!.id).isEqualTo(log2Latest.id)
  }

  @Test
  fun `findHeartbeatHistory returns heartbeats within time range`() {
    val dataplaneId = UUID.randomUUID()
    val now = OffsetDateTime.now()

    // Create multiple heartbeats
    val log1 =
      dataplaneHeartbeatLogRepository.save(
        DataplaneHeartbeatLog(
          dataplaneId = dataplaneId,
          controlPlaneVersion = "1.0.0",
          dataplaneVersion = "2.0.0",
        ),
      )

    Thread.sleep(10)

    val log2 =
      dataplaneHeartbeatLogRepository.save(
        DataplaneHeartbeatLog(
          dataplaneId = dataplaneId,
          controlPlaneVersion = "1.0.1",
          dataplaneVersion = "2.0.1",
        ),
      )

    Thread.sleep(10)

    val log3 =
      dataplaneHeartbeatLogRepository.save(
        DataplaneHeartbeatLog(
          dataplaneId = dataplaneId,
          controlPlaneVersion = "1.0.2",
          dataplaneVersion = "2.0.2",
        ),
      )

    // Query for all heartbeats
    val allHistory =
      dataplaneHeartbeatLogRepository.findHeartbeatHistory(
        dataplaneId = dataplaneId,
        startTime = now.minusMinutes(1),
        endTime = now.plusMinutes(1),
      )

    assertEquals(3, allHistory.size)

    // Query for heartbeats after log1's creation
    val partialHistory =
      dataplaneHeartbeatLogRepository.findHeartbeatHistory(
        dataplaneId = dataplaneId,
        startTime = log1.createdAt!!.plusNanos(1000000), // Add 1ms to exclude log1
        endTime = now.plusMinutes(1),
      )

    assertEquals(2, partialHistory.size)
    assertThat(partialHistory.map { it.id }).containsExactlyInAnyOrder(log2.id, log3.id)
  }

  @Test
  fun `findHeartbeatHistory returns results ordered by created_at desc`() {
    val dataplaneId = UUID.randomUUID()
    val now = OffsetDateTime.now()

    // Create heartbeats in order
    val log1 =
      dataplaneHeartbeatLogRepository.save(
        DataplaneHeartbeatLog(
          dataplaneId = dataplaneId,
          controlPlaneVersion = "1.0.0",
          dataplaneVersion = "2.0.0",
        ),
      )

    Thread.sleep(10)

    val log2 =
      dataplaneHeartbeatLogRepository.save(
        DataplaneHeartbeatLog(
          dataplaneId = dataplaneId,
          controlPlaneVersion = "1.0.1",
          dataplaneVersion = "2.0.1",
        ),
      )

    val history =
      dataplaneHeartbeatLogRepository.findHeartbeatHistory(
        dataplaneId = dataplaneId,
        startTime = now.minusMinutes(1),
        endTime = now.plusMinutes(1),
      )

    // Should be ordered by created_at DESC (newest first)
    assertEquals(2, history.size)
    assertEquals(log2.id, history[0].id)
    assertEquals(log1.id, history[1].id)
  }

  @Test
  fun `findHeartbeatHistoryForDataplanes returns heartbeats for multiple dataplanes`() {
    val dataplaneId1 = UUID.randomUUID()
    val dataplaneId2 = UUID.randomUUID()
    val dataplaneId3 = UUID.randomUUID()
    val now = OffsetDateTime.now()

    // Create heartbeats for dataplane 1
    val log1a =
      dataplaneHeartbeatLogRepository.save(
        DataplaneHeartbeatLog(
          dataplaneId = dataplaneId1,
          controlPlaneVersion = "1.0.0",
          dataplaneVersion = "2.0.0",
        ),
      )

    Thread.sleep(10)

    val log1b =
      dataplaneHeartbeatLogRepository.save(
        DataplaneHeartbeatLog(
          dataplaneId = dataplaneId1,
          controlPlaneVersion = "1.0.1",
          dataplaneVersion = "2.0.1",
        ),
      )

    // Create heartbeats for dataplane 2
    val log2a =
      dataplaneHeartbeatLogRepository.save(
        DataplaneHeartbeatLog(
          dataplaneId = dataplaneId2,
          controlPlaneVersion = "1.1.0",
          dataplaneVersion = "2.1.0",
        ),
      )

    Thread.sleep(10)

    val log2b =
      dataplaneHeartbeatLogRepository.save(
        DataplaneHeartbeatLog(
          dataplaneId = dataplaneId2,
          controlPlaneVersion = "1.1.1",
          dataplaneVersion = "2.1.1",
        ),
      )

    // Create heartbeat for dataplane 3
    val log3 =
      dataplaneHeartbeatLogRepository.save(
        DataplaneHeartbeatLog(
          dataplaneId = dataplaneId3,
          controlPlaneVersion = "1.2.0",
          dataplaneVersion = "2.2.0",
        ),
      )

    // Query for all three dataplanes
    val allHistory =
      dataplaneHeartbeatLogRepository.findHeartbeatHistoryForDataplanes(
        dataplaneIds = listOf(dataplaneId1, dataplaneId2, dataplaneId3),
        startTime = now.minusMinutes(1),
        endTime = now.plusMinutes(1),
      )

    assertEquals(5, allHistory.size)

    // Group by dataplane and verify each has expected logs
    val historyByDataplane = allHistory.groupBy { it.dataplaneId }

    assertEquals(2, historyByDataplane[dataplaneId1]?.size)
    assertThat(historyByDataplane[dataplaneId1]?.map { it.id })
      .containsExactlyInAnyOrder(log1a.id, log1b.id)

    assertEquals(2, historyByDataplane[dataplaneId2]?.size)
    assertThat(historyByDataplane[dataplaneId2]?.map { it.id })
      .containsExactlyInAnyOrder(log2a.id, log2b.id)

    assertEquals(1, historyByDataplane[dataplaneId3]?.size)
    assertEquals(log3.id, historyByDataplane[dataplaneId3]?.get(0)?.id)
  }

  @Test
  fun `findHeartbeatHistoryForDataplanes returns results ordered by created_at desc within each dataplane`() {
    val dataplaneId1 = UUID.randomUUID()
    val dataplaneId2 = UUID.randomUUID()
    val now = OffsetDateTime.now()

    // Create heartbeats for dataplane 1
    val log1a =
      dataplaneHeartbeatLogRepository.save(
        DataplaneHeartbeatLog(
          dataplaneId = dataplaneId1,
          controlPlaneVersion = "1.0.0",
          dataplaneVersion = "2.0.0",
        ),
      )

    Thread.sleep(10)

    val log1b =
      dataplaneHeartbeatLogRepository.save(
        DataplaneHeartbeatLog(
          dataplaneId = dataplaneId1,
          controlPlaneVersion = "1.0.1",
          dataplaneVersion = "2.0.1",
        ),
      )

    // Create heartbeats for dataplane 2
    val log2a =
      dataplaneHeartbeatLogRepository.save(
        DataplaneHeartbeatLog(
          dataplaneId = dataplaneId2,
          controlPlaneVersion = "1.1.0",
          dataplaneVersion = "2.1.0",
        ),
      )

    Thread.sleep(10)

    val log2b =
      dataplaneHeartbeatLogRepository.save(
        DataplaneHeartbeatLog(
          dataplaneId = dataplaneId2,
          controlPlaneVersion = "1.1.1",
          dataplaneVersion = "2.1.1",
        ),
      )

    val history =
      dataplaneHeartbeatLogRepository.findHeartbeatHistoryForDataplanes(
        dataplaneIds = listOf(dataplaneId1, dataplaneId2),
        startTime = now.minusMinutes(1),
        endTime = now.plusMinutes(1),
      )

    assertEquals(4, history.size)

    // Group by dataplane to verify ordering within each group
    val historyByDataplane = history.groupBy { it.dataplaneId }

    // Within each dataplane, should be ordered by created_at DESC (newest first)
    val dataplane1History = historyByDataplane[dataplaneId1]!!
    assertEquals(log1b.id, dataplane1History[0].id)
    assertEquals(log1a.id, dataplane1History[1].id)

    val dataplane2History = historyByDataplane[dataplaneId2]!!
    assertEquals(log2b.id, dataplane2History[0].id)
    assertEquals(log2a.id, dataplane2History[1].id)
  }

  @Test
  fun `findHeartbeatHistoryForDataplanes respects time range filter`() {
    val dataplaneId1 = UUID.randomUUID()
    val dataplaneId2 = UUID.randomUUID()
    val now = OffsetDateTime.now()

    // Create an old heartbeat for dataplane 1
    jooqDslContext
      .insertInto(Tables.DATAPLANE_HEARTBEAT_LOG)
      .columns(
        Tables.DATAPLANE_HEARTBEAT_LOG.ID,
        Tables.DATAPLANE_HEARTBEAT_LOG.DATAPLANE_ID,
        Tables.DATAPLANE_HEARTBEAT_LOG.CONTROL_PLANE_VERSION,
        Tables.DATAPLANE_HEARTBEAT_LOG.DATAPLANE_VERSION,
        Tables.DATAPLANE_HEARTBEAT_LOG.CREATED_AT,
      ).values(
        UUID.randomUUID(),
        dataplaneId1,
        "1.0.0",
        "2.0.0",
        now.minusMinutes(10),
      ).execute()

    // Create recent heartbeats
    val log1 =
      dataplaneHeartbeatLogRepository.save(
        DataplaneHeartbeatLog(
          dataplaneId = dataplaneId1,
          controlPlaneVersion = "1.0.1",
          dataplaneVersion = "2.0.1",
        ),
      )

    val log2 =
      dataplaneHeartbeatLogRepository.save(
        DataplaneHeartbeatLog(
          dataplaneId = dataplaneId2,
          controlPlaneVersion = "1.1.0",
          dataplaneVersion = "2.1.0",
        ),
      )

    // Query for only recent heartbeats (last 5 minutes)
    val recentHistory =
      dataplaneHeartbeatLogRepository.findHeartbeatHistoryForDataplanes(
        dataplaneIds = listOf(dataplaneId1, dataplaneId2),
        startTime = now.minusMinutes(5),
        endTime = now.plusMinutes(1),
      )

    assertEquals(2, recentHistory.size)
    assertThat(recentHistory.map { it.id }).containsExactlyInAnyOrder(log1.id, log2.id)
  }

  @Test
  fun `findHeartbeatHistoryForDataplanes returns empty list when no heartbeats in time range`() {
    val dataplaneId = UUID.randomUUID()
    val now = OffsetDateTime.now()

    dataplaneHeartbeatLogRepository.save(
      DataplaneHeartbeatLog(
        dataplaneId = dataplaneId,
        controlPlaneVersion = "1.0.0",
        dataplaneVersion = "2.0.0",
      ),
    )

    // Query for heartbeats in the future
    val history =
      dataplaneHeartbeatLogRepository.findHeartbeatHistoryForDataplanes(
        dataplaneIds = listOf(dataplaneId),
        startTime = now.plusMinutes(10),
        endTime = now.plusMinutes(20),
      )

    assertEquals(0, history.size)
  }

  @Test
  fun `findLatestHeartbeatsByDataplaneIds returns empty list when no heartbeats exist`() {
    val dataplaneId = UUID.randomUUID()

    val latest = dataplaneHeartbeatLogRepository.findLatestHeartbeatsByDataplaneIds(listOf(dataplaneId))

    assertEquals(0, latest.size)
  }

  @Test
  fun `deleteOldHeartbeatsExceptLatest deletes records older than cutoff`() {
    val dataplaneId = UUID.randomUUID()

    // Insert old log with manual timestamp
    jooqDslContext
      .insertInto(Tables.DATAPLANE_HEARTBEAT_LOG)
      .columns(
        Tables.DATAPLANE_HEARTBEAT_LOG.ID,
        Tables.DATAPLANE_HEARTBEAT_LOG.DATAPLANE_ID,
        Tables.DATAPLANE_HEARTBEAT_LOG.CONTROL_PLANE_VERSION,
        Tables.DATAPLANE_HEARTBEAT_LOG.DATAPLANE_VERSION,
        Tables.DATAPLANE_HEARTBEAT_LOG.CREATED_AT,
      ).values(
        UUID.randomUUID(),
        dataplaneId,
        "1.0.0",
        "2.0.0",
        OffsetDateTime.now().minusHours(25),
      ).execute()

    // Insert recent log
    val recentLog =
      dataplaneHeartbeatLogRepository.save(
        DataplaneHeartbeatLog(
          dataplaneId = dataplaneId,
          controlPlaneVersion = "1.0.1",
          dataplaneVersion = "2.0.1",
        ),
      )

    val cutoffTime = OffsetDateTime.now().minusHours(24)
    val deletedCount = dataplaneHeartbeatLogRepository.deleteOldHeartbeatsExceptLatest(cutoffTime)

    assertEquals(1, deletedCount)

    val remainingLogs = dataplaneHeartbeatLogRepository.findAll().toList()
    assertEquals(1, remainingLogs.size)
    assertEquals(recentLog.id, remainingLogs[0].id)
  }

  @Test
  fun `deleteOldHeartbeatsExceptLatest preserves most recent log even if older than cutoff`() {
    val dataplaneId = UUID.randomUUID()

    // Insert very old log with manual timestamp
    jooqDslContext
      .insertInto(Tables.DATAPLANE_HEARTBEAT_LOG)
      .columns(
        Tables.DATAPLANE_HEARTBEAT_LOG.ID,
        Tables.DATAPLANE_HEARTBEAT_LOG.DATAPLANE_ID,
        Tables.DATAPLANE_HEARTBEAT_LOG.CONTROL_PLANE_VERSION,
        Tables.DATAPLANE_HEARTBEAT_LOG.DATAPLANE_VERSION,
        Tables.DATAPLANE_HEARTBEAT_LOG.CREATED_AT,
      ).values(
        UUID.randomUUID(),
        dataplaneId,
        "1.0.0",
        "2.0.0",
        OffsetDateTime.now().minusDays(30),
      ).execute()

    val cutoffTime = OffsetDateTime.now().minusHours(24)
    val deletedCount = dataplaneHeartbeatLogRepository.deleteOldHeartbeatsExceptLatest(cutoffTime)

    assertEquals(0, deletedCount)

    val remainingLogs = dataplaneHeartbeatLogRepository.findAll().toList()
    assertEquals(1, remainingLogs.size)
    assertEquals(dataplaneId, remainingLogs[0].dataplaneId)
  }

  @Test
  fun `deleteOldHeartbeatsExceptLatest handles multiple dataplanes correctly`() {
    val dataplane1 = UUID.randomUUID()
    val dataplane2 = UUID.randomUUID()

    // Insert old logs with manual timestamps
    jooqDslContext
      .insertInto(Tables.DATAPLANE_HEARTBEAT_LOG)
      .columns(
        Tables.DATAPLANE_HEARTBEAT_LOG.ID,
        Tables.DATAPLANE_HEARTBEAT_LOG.DATAPLANE_ID,
        Tables.DATAPLANE_HEARTBEAT_LOG.CONTROL_PLANE_VERSION,
        Tables.DATAPLANE_HEARTBEAT_LOG.DATAPLANE_VERSION,
        Tables.DATAPLANE_HEARTBEAT_LOG.CREATED_AT,
      ).values(
        UUID.randomUUID(),
        dataplane1,
        "1.0.0",
        "2.0.0",
        OffsetDateTime.now().minusHours(30),
      ).values(
        UUID.randomUUID(),
        dataplane1,
        "1.0.1",
        "2.0.1",
        OffsetDateTime.now().minusHours(26),
      ).values(
        UUID.randomUUID(),
        dataplane2,
        "1.0.0",
        "2.0.0",
        OffsetDateTime.now().minusDays(60),
      ).values(
        UUID.randomUUID(),
        dataplane2,
        "1.0.1",
        "2.0.1",
        OffsetDateTime.now().minusDays(50),
      ).execute()

    // Insert recent log for dataplane1
    val dataplane1Latest =
      dataplaneHeartbeatLogRepository.save(
        DataplaneHeartbeatLog(
          dataplaneId = dataplane1,
          controlPlaneVersion = "1.0.2",
          dataplaneVersion = "2.0.2",
        ),
      )

    val cutoffTime = OffsetDateTime.now().minusHours(24)
    val deletedCount = dataplaneHeartbeatLogRepository.deleteOldHeartbeatsExceptLatest(cutoffTime)

    assertEquals(3, deletedCount)

    val remainingLogs = dataplaneHeartbeatLogRepository.findAll().toList()
    assertEquals(2, remainingLogs.size)

    val remainingIds = remainingLogs.map { it.id }.toSet()
    assertThat(remainingIds).contains(dataplane1Latest.id)
    // dataplane2 has no recent records, but its most recent (50 days old) should be kept
    assertThat(remainingLogs.any { it.dataplaneId == dataplane2 }).isTrue()
  }

  @Test
  fun `deleteOldHeartbeatsExceptLatest returns zero when no logs exist`() {
    val cutoffTime = OffsetDateTime.now().minusHours(24)
    val deletedCount = dataplaneHeartbeatLogRepository.deleteOldHeartbeatsExceptLatest(cutoffTime)

    assertEquals(0, deletedCount)
  }

  @Test
  fun `deleteOldHeartbeatsExceptLatest returns zero when all logs are recent`() {
    val dataplaneId = UUID.randomUUID()

    dataplaneHeartbeatLogRepository.save(
      DataplaneHeartbeatLog(
        dataplaneId = dataplaneId,
        controlPlaneVersion = "1.0.0",
        dataplaneVersion = "2.0.0",
      ),
    )

    dataplaneHeartbeatLogRepository.save(
      DataplaneHeartbeatLog(
        dataplaneId = dataplaneId,
        controlPlaneVersion = "1.0.1",
        dataplaneVersion = "2.0.1",
      ),
    )

    val cutoffTime = OffsetDateTime.now().minusHours(24)
    val deletedCount = dataplaneHeartbeatLogRepository.deleteOldHeartbeatsExceptLatest(cutoffTime)

    assertEquals(0, deletedCount)

    val remainingLogs = dataplaneHeartbeatLogRepository.findAll().toList()
    assertEquals(2, remainingLogs.size)
  }
}
