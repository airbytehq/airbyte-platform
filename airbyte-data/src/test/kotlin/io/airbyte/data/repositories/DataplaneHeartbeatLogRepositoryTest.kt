/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.DataplaneHeartbeatLog
import io.airbyte.db.instance.configs.jooq.generated.Keys
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.assertj.core.api.Assertions.assertThat
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
  fun `findLatestHeartbeatsByDataplaneIds returns empty list when no heartbeats exist`() {
    val dataplaneId = UUID.randomUUID()

    val latest = dataplaneHeartbeatLogRepository.findLatestHeartbeatsByDataplaneIds(listOf(dataplaneId))

    assertEquals(0, latest.size)
  }
}
