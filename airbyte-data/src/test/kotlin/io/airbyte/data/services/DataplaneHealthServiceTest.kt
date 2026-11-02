/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.data.repositories.DataplaneHeartbeatLogRepository
import io.airbyte.data.repositories.entities.DataplaneHeartbeatLog
import io.airbyte.data.services.DataplaneHealthService.Companion.HISTORY_BUCKET_COUNT
import io.airbyte.data.services.DataplaneHealthService.Companion.HISTORY_BUCKET_DURATION
import io.airbyte.data.services.DataplaneHealthService.Companion.HISTORY_WINDOW_DURATION
import io.airbyte.data.services.DataplaneHealthService.Companion.RETENTION_PERIOD_DURATION
import io.airbyte.data.services.DataplaneHealthService.Companion.UNKNOWN_VERSION
import io.airbyte.data.services.DataplaneHealthService.Companion.historyWindowStart
import io.airbyte.domain.models.DataplaneHealthInfo.HealthStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.OffsetDateTime
import java.util.UUID

internal class DataplaneHealthServiceTest {
  private val heartbeatLogRepository = mockk<DataplaneHeartbeatLogRepository>(relaxed = true)
  private val dataplaneHealthService = DataplaneHealthService(heartbeatLogRepository)

  @Test
  fun `recordHeartbeat saves log with correct fields`() {
    val dataplaneId = UUID.randomUUID()
    val controlPlaneVersion = "1.2.3"
    val dataplaneVersion = "2.3.4"

    val logSlot = slot<DataplaneHeartbeatLog>()

    dataplaneHealthService.recordHeartbeat(
      dataplaneId = dataplaneId,
      controlPlaneVersion = controlPlaneVersion,
      dataplaneVersion = dataplaneVersion,
    )

    verify { heartbeatLogRepository.save(capture(logSlot)) }

    val capturedLog = logSlot.captured
    assertEquals(dataplaneId, capturedLog.dataplaneId)
    assertEquals(controlPlaneVersion, capturedLog.controlPlaneVersion)
    assertEquals(dataplaneVersion, capturedLog.dataplaneVersion)
  }

  @Test
  fun `recordHeartbeat defaults to unknown when versions are null`() {
    val dataplaneId = UUID.randomUUID()
    val logSlot = slot<DataplaneHeartbeatLog>()

    dataplaneHealthService.recordHeartbeat(
      dataplaneId = dataplaneId,
    )

    verify { heartbeatLogRepository.save(capture(logSlot)) }

    val capturedLog = logSlot.captured
    assertEquals(dataplaneId, capturedLog.dataplaneId)
    assertEquals(UNKNOWN_VERSION, capturedLog.controlPlaneVersion)
    assertEquals(UNKNOWN_VERSION, capturedLog.dataplaneVersion)
  }

  @Test
  fun `recordHeartbeat can be called multiple times`() {
    val dataplaneId = UUID.randomUUID()

    dataplaneHealthService.recordHeartbeat(
      dataplaneId = dataplaneId,
      controlPlaneVersion = "1.0.0",
      dataplaneVersion = "2.0.0",
    )

    dataplaneHealthService.recordHeartbeat(
      dataplaneId = dataplaneId,
      controlPlaneVersion = "1.0.1",
      dataplaneVersion = "2.0.1",
    )

    verify(exactly = 2) { heartbeatLogRepository.save(any()) }
  }

  @Test
  fun `getDataplaneHealthInfos returns health for multiple dataplanes`() {
    val dataplane1 = UUID.randomUUID()
    val dataplane2 = UUID.randomUUID()
    val dataplane3 = UUID.randomUUID()

    val heartbeat1 =
      DataplaneHeartbeatLog(
        id = UUID.randomUUID(),
        dataplaneId = dataplane1,
        controlPlaneVersion = "1.0.0",
        dataplaneVersion = "2.0.0",
        createdAt = OffsetDateTime.now().minusSeconds(30),
      )

    val heartbeat2 =
      DataplaneHeartbeatLog(
        id = UUID.randomUUID(),
        dataplaneId = dataplane2,
        controlPlaneVersion = "1.0.1",
        dataplaneVersion = "2.0.1",
        createdAt = OffsetDateTime.now().minusMinutes(10),
      )

    every {
      heartbeatLogRepository.findLatestHeartbeatsByDataplaneIds(
        listOf(dataplane1, dataplane2, dataplane3),
      )
    } returns listOf(heartbeat1, heartbeat2)

    val healthStatuses = dataplaneHealthService.getDataplaneHealthInfos(listOf(dataplane1, dataplane2, dataplane3), includeHistory = false)

    assertEquals(3, healthStatuses.size)
    assertEquals(HealthStatus.HEALTHY, healthStatuses[0].status)
    assertEquals(HealthStatus.UNHEALTHY, healthStatuses[1].status)
    assertEquals(HealthStatus.UNKNOWN, healthStatuses[2].status)
  }

  @Test
  fun `getDataplaneHealthStatuses returns empty list for empty input`() {
    val healthStatuses = dataplaneHealthService.getDataplaneHealthInfos(emptyList(), includeHistory = false)

    assertEquals(0, healthStatuses.size)
  }

  @Test
  fun `getDataplaneHealthInfos without history keeps the recent heartbeat read and returns no history`() {
    val dataplaneId = UUID.randomUUID()
    val now = OffsetDateTime.now()
    val recentLogs = heartbeatLogsAt(dataplaneId, listOf(now.minusSeconds(90), now.minusSeconds(30)))
    every { heartbeatLogRepository.findLatestHeartbeatsByDataplaneIds(listOf(dataplaneId)) } returns listOf(recentLogs.first())
    every { heartbeatLogRepository.findHeartbeatHistoryForDataplanes(listOf(dataplaneId), any(), any()) } returns recentLogs

    val healthInfo = dataplaneHealthService.getDataplaneHealthInfos(listOf(dataplaneId), includeHistory = false).single()

    assertEquals(HealthStatus.HEALTHY, healthInfo.status)
    assertEquals(2, healthInfo.recentHeartbeats.size)
    assertTrue(healthInfo.healthHistory.isEmpty())
    verify(exactly = 1) {
      heartbeatLogRepository.findHeartbeatHistoryForDataplanes(
        listOf(dataplaneId),
        match { it.isAfter(now.minus(DataplaneHealthService.DEGRADED_THRESHOLD_DURATION).minusSeconds(5)) },
        any(),
      )
    }
    verify(exactly = 0) { heartbeatLogRepository.findLatestHeartbeatsBeforeForDataplanes(any(), any(), any()) }
  }

  @Test
  fun `history with no heartbeats at all is 48 UNKNOWN periods oldest first`() {
    val dataplaneId = UUID.randomUUID()
    every { heartbeatLogRepository.findLatestHeartbeatsByDataplaneIds(listOf(dataplaneId)) } returns emptyList()
    every { heartbeatLogRepository.findHeartbeatHistoryForDataplanes(listOf(dataplaneId), any(), any()) } returns emptyList()
    every { heartbeatLogRepository.findLatestHeartbeatsBeforeForDataplanes(listOf(dataplaneId), any(), any()) } returns emptyList()

    val healthInfo = dataplaneHealthService.getDataplaneHealthInfos(listOf(dataplaneId), includeHistory = true).single()

    assertEquals(HealthStatus.UNKNOWN, healthInfo.status)
    assertEquals(HISTORY_BUCKET_COUNT, healthInfo.healthHistory.size)
    assertEquals(48, healthInfo.healthHistory.size)
    assertTrue(healthInfo.healthHistory.all { it.status == HealthStatus.UNKNOWN })
    healthInfo.healthHistory.zipWithNext { earlier, later ->
      assertEquals(HISTORY_BUCKET_DURATION, Duration.between(earlier.bucketStart, later.bucketStart))
    }
    val windowSpan = Duration.between(healthInfo.healthHistory.first().bucketStart, healthInfo.healthHistory.last().bucketStart)
    assertEquals(HISTORY_WINDOW_DURATION.minus(HISTORY_BUCKET_DURATION), windowSpan)
    verify(exactly = 1) {
      heartbeatLogRepository.findHeartbeatHistoryForDataplanes(
        listOf(dataplaneId),
        match { it == historyWindowStart(OffsetDateTime.now()) },
        any(),
      )
    }
  }

  @Test
  fun `history periods are aligned to the clock and end with the current quarter`() {
    val dataplaneId = UUID.randomUUID()
    every { heartbeatLogRepository.findLatestHeartbeatsByDataplaneIds(listOf(dataplaneId)) } returns emptyList()
    every { heartbeatLogRepository.findHeartbeatHistoryForDataplanes(listOf(dataplaneId), any(), any()) } returns emptyList()
    every { heartbeatLogRepository.findLatestHeartbeatsBeforeForDataplanes(listOf(dataplaneId), any(), any()) } returns emptyList()

    val healthInfo = dataplaneHealthService.getDataplaneHealthInfos(listOf(dataplaneId), includeHistory = true).single()

    healthInfo.healthHistory.forEach { bucket ->
      assertEquals(0, bucket.bucketStart.toInstant().toEpochMilli() % HISTORY_BUCKET_DURATION.toMillis())
    }
    val lastStart = healthInfo.healthHistory.last().bucketStart
    val now = OffsetDateTime.now()
    assertTrue(!lastStart.isAfter(now))
    assertTrue(lastStart.plus(HISTORY_BUCKET_DURATION).isAfter(now))
  }

  @Test
  fun `history of a dataplane whose newest check-in is older than the window is UNHEALTHY throughout`() {
    val dataplaneId = UUID.randomUUID()
    val now = OffsetDateTime.now()
    val lastSeen = heartbeatLogsAt(dataplaneId, listOf(now.minusDays(2))).single()
    every { heartbeatLogRepository.findLatestHeartbeatsByDataplaneIds(listOf(dataplaneId)) } returns listOf(lastSeen)
    every { heartbeatLogRepository.findHeartbeatHistoryForDataplanes(listOf(dataplaneId), any(), any()) } returns emptyList()
    every { heartbeatLogRepository.findLatestHeartbeatsBeforeForDataplanes(listOf(dataplaneId), any(), any()) } returns emptyList()

    val healthInfo = dataplaneHealthService.getDataplaneHealthInfos(listOf(dataplaneId), includeHistory = true).single()

    assertEquals(HealthStatus.UNHEALTHY, healthInfo.status)
    assertEquals(HISTORY_BUCKET_COUNT, healthInfo.healthHistory.size)
    assertTrue(healthInfo.healthHistory.all { it.status == HealthStatus.UNHEALTHY })
  }

  @Test
  fun `history grades one period UNHEALTHY when a silence longer than five minutes happens inside it`() {
    val dataplaneId = UUID.randomUUID()
    val now = OffsetDateTime.now()
    val windowStart = historyWindowStart(now)
    val silentBucket = 30
    val silenceStart = windowStart.plus(HISTORY_BUCKET_DURATION.multipliedBy(silentBucket.toLong())).plusMinutes(2)
    val silenceEnd = silenceStart.plusMinutes(6)
    val checkIns = everyThirtySeconds(windowStart, now).filterNot { it.isAfter(silenceStart) && it.isBefore(silenceEnd) }
    val logs = heartbeatLogsAt(dataplaneId, checkIns)
    every { heartbeatLogRepository.findLatestHeartbeatsByDataplaneIds(listOf(dataplaneId)) } returns listOf(logs.first())
    every { heartbeatLogRepository.findHeartbeatHistoryForDataplanes(listOf(dataplaneId), any(), any()) } returns logs
    every { heartbeatLogRepository.findLatestHeartbeatsBeforeForDataplanes(listOf(dataplaneId), any(), any()) } returns emptyList()

    val healthInfo = dataplaneHealthService.getDataplaneHealthInfos(listOf(dataplaneId), includeHistory = true).single()

    assertEquals(HealthStatus.HEALTHY, healthInfo.status)
    val statuses = healthInfo.healthHistory.map { it.status }
    assertEquals(HealthStatus.UNHEALTHY, statuses[silentBucket])
    assertEquals(listOf(silentBucket), statuses.withIndex().filter { it.value != HealthStatus.HEALTHY }.map { it.index })
  }

  @Test
  fun `history grades a period DEGRADED when its longest silence is between one and five minutes`() {
    val dataplaneId = UUID.randomUUID()
    val now = OffsetDateTime.now()
    val windowStart = historyWindowStart(now)
    val degradedBucket = 10
    val silenceStart = windowStart.plus(HISTORY_BUCKET_DURATION.multipliedBy(degradedBucket.toLong())).plusMinutes(5)
    val silenceEnd = silenceStart.plusMinutes(3)
    val checkIns = everyThirtySeconds(windowStart, now).filterNot { it.isAfter(silenceStart) && it.isBefore(silenceEnd) }
    val logs = heartbeatLogsAt(dataplaneId, checkIns)
    every { heartbeatLogRepository.findLatestHeartbeatsByDataplaneIds(listOf(dataplaneId)) } returns listOf(logs.first())
    every { heartbeatLogRepository.findHeartbeatHistoryForDataplanes(listOf(dataplaneId), any(), any()) } returns logs
    every { heartbeatLogRepository.findLatestHeartbeatsBeforeForDataplanes(listOf(dataplaneId), any(), any()) } returns emptyList()

    val healthInfo = dataplaneHealthService.getDataplaneHealthInfos(listOf(dataplaneId), includeHistory = true).single()

    val statuses = healthInfo.healthHistory.map { it.status }
    assertEquals(HealthStatus.DEGRADED, statuses[degradedBucket])
    assertEquals(listOf(degradedBucket), statuses.withIndex().filter { it.value != HealthStatus.HEALTHY }.map { it.index })
  }

  @Test
  fun `history uses a check-in before the window to keep the first period HEALTHY`() {
    val dataplaneId = UUID.randomUUID()
    val now = OffsetDateTime.now()
    val windowStart = historyWindowStart(now)
    val checkInBeforeWindow = heartbeatLogsAt(dataplaneId, listOf(windowStart.minusSeconds(20))).single()
    val logs = heartbeatLogsAt(dataplaneId, everyThirtySeconds(windowStart, now))
    every { heartbeatLogRepository.findLatestHeartbeatsByDataplaneIds(listOf(dataplaneId)) } returns listOf(logs.first())
    every { heartbeatLogRepository.findHeartbeatHistoryForDataplanes(listOf(dataplaneId), any(), any()) } returns logs
    every { heartbeatLogRepository.findLatestHeartbeatsBeforeForDataplanes(listOf(dataplaneId), any(), any()) } returns listOf(checkInBeforeWindow)

    val healthInfo = dataplaneHealthService.getDataplaneHealthInfos(listOf(dataplaneId), includeHistory = true).single()

    assertEquals(HealthStatus.HEALTHY, healthInfo.healthHistory.first().status)
    assertTrue(healthInfo.healthHistory.all { it.status == HealthStatus.HEALTHY })
  }

  @Test
  fun `history carries a long silence across the window start as UNHEALTHY`() {
    val dataplaneId = UUID.randomUUID()
    val now = OffsetDateTime.now()
    val windowStart = historyWindowStart(now)
    val checkInBeforeWindow = heartbeatLogsAt(dataplaneId, listOf(windowStart.minusMinutes(10))).single()
    val logs = heartbeatLogsAt(dataplaneId, everyThirtySeconds(windowStart.plus(HISTORY_BUCKET_DURATION), now))
    every { heartbeatLogRepository.findLatestHeartbeatsByDataplaneIds(listOf(dataplaneId)) } returns listOf(logs.first())
    every { heartbeatLogRepository.findHeartbeatHistoryForDataplanes(listOf(dataplaneId), any(), any()) } returns logs
    every { heartbeatLogRepository.findLatestHeartbeatsBeforeForDataplanes(listOf(dataplaneId), any(), any()) } returns listOf(checkInBeforeWindow)

    val healthInfo = dataplaneHealthService.getDataplaneHealthInfos(listOf(dataplaneId), includeHistory = true).single()

    val statuses = healthInfo.healthHistory.map { it.status }
    assertEquals(HealthStatus.UNHEALTHY, statuses[0])
    assertEquals(HealthStatus.UNHEALTHY, statuses[1])
    assertTrue(statuses.drop(2).all { it == HealthStatus.HEALTHY })
  }

  @Test
  fun `history with less than 12 hours of data is UNKNOWN before the first check-in and graded after it`() {
    val dataplaneId = UUID.randomUUID()
    val now = OffsetDateTime.now()
    val windowStart = historyWindowStart(now)
    val firstCheckIn = windowStart.plus(HISTORY_BUCKET_DURATION.multipliedBy(40)).plusMinutes(7)
    val logs = heartbeatLogsAt(dataplaneId, everyThirtySeconds(firstCheckIn, now))
    every { heartbeatLogRepository.findLatestHeartbeatsByDataplaneIds(listOf(dataplaneId)) } returns listOf(logs.first())
    every { heartbeatLogRepository.findHeartbeatHistoryForDataplanes(listOf(dataplaneId), any(), any()) } returns logs
    every { heartbeatLogRepository.findLatestHeartbeatsBeforeForDataplanes(listOf(dataplaneId), any(), any()) } returns emptyList()

    val healthInfo = dataplaneHealthService.getDataplaneHealthInfos(listOf(dataplaneId), includeHistory = true).single()

    val statuses = healthInfo.healthHistory.map { it.status }
    assertTrue(statuses.take(40).all { it == HealthStatus.UNKNOWN })
    assertTrue(statuses.drop(40).all { it == HealthStatus.HEALTHY })
  }

  @Test
  fun `history grades a period with no check-ins by the silence since the last check-in`() {
    val dataplaneId = UUID.randomUUID()
    val now = OffsetDateTime.now()
    val windowStart = historyWindowStart(now)
    val lastCheckIn = windowStart.plus(HISTORY_BUCKET_DURATION.multipliedBy(20)).plusMinutes(1)
    val logs = heartbeatLogsAt(dataplaneId, everyThirtySeconds(windowStart, lastCheckIn))
    every { heartbeatLogRepository.findLatestHeartbeatsByDataplaneIds(listOf(dataplaneId)) } returns listOf(logs.first())
    every { heartbeatLogRepository.findHeartbeatHistoryForDataplanes(listOf(dataplaneId), any(), any()) } returns logs
    every { heartbeatLogRepository.findLatestHeartbeatsBeforeForDataplanes(listOf(dataplaneId), any(), any()) } returns emptyList()

    val healthInfo = dataplaneHealthService.getDataplaneHealthInfos(listOf(dataplaneId), includeHistory = true).single()

    assertEquals(HealthStatus.UNHEALTHY, healthInfo.status)
    val statuses = healthInfo.healthHistory.map { it.status }
    assertTrue(statuses.take(20).all { it == HealthStatus.HEALTHY })
    assertTrue(statuses.drop(20).all { it == HealthStatus.UNHEALTHY })
  }

  @Test
  fun `history is computed per dataplane from one heartbeat read`() {
    val healthyDataplane = UUID.randomUUID()
    val silentDataplane = UUID.randomUUID()
    val now = OffsetDateTime.now()
    val windowStart = historyWindowStart(now)
    val healthyLogs = heartbeatLogsAt(healthyDataplane, everyThirtySeconds(windowStart, now))
    every { heartbeatLogRepository.findLatestHeartbeatsByDataplaneIds(listOf(healthyDataplane, silentDataplane)) } returns listOf(healthyLogs.first())
    every { heartbeatLogRepository.findHeartbeatHistoryForDataplanes(listOf(healthyDataplane, silentDataplane), any(), any()) } returns healthyLogs
    every { heartbeatLogRepository.findLatestHeartbeatsBeforeForDataplanes(listOf(healthyDataplane, silentDataplane), any(), any()) } returns
      emptyList()

    val healthInfos = dataplaneHealthService.getDataplaneHealthInfos(listOf(healthyDataplane, silentDataplane), includeHistory = true)

    assertTrue(healthInfos[0].healthHistory.all { it.status == HealthStatus.HEALTHY })
    assertTrue(healthInfos[1].healthHistory.all { it.status == HealthStatus.UNKNOWN })
    verify(exactly = 1) { heartbeatLogRepository.findHeartbeatHistoryForDataplanes(any(), any(), any()) }
    verify(exactly = 1) { heartbeatLogRepository.findLatestHeartbeatsBeforeForDataplanes(any(), any(), any()) }
  }

  @Test
  fun `history looks for the check-in before the window only inside the retention period`() {
    val dataplaneId = UUID.randomUUID()
    every { heartbeatLogRepository.findLatestHeartbeatsByDataplaneIds(listOf(dataplaneId)) } returns emptyList()
    every { heartbeatLogRepository.findHeartbeatHistoryForDataplanes(listOf(dataplaneId), any(), any()) } returns emptyList()
    every { heartbeatLogRepository.findLatestHeartbeatsBeforeForDataplanes(listOf(dataplaneId), any(), any()) } returns emptyList()

    dataplaneHealthService.getDataplaneHealthInfos(listOf(dataplaneId), includeHistory = true)

    verify {
      heartbeatLogRepository.findLatestHeartbeatsBeforeForDataplanes(
        listOf(dataplaneId),
        match {
          Duration
            .between(it, OffsetDateTime.now())
            .abs()
            .minus(RETENTION_PERIOD_DURATION)
            .abs() < Duration.ofSeconds(5)
        },
        match { it == historyWindowStart(OffsetDateTime.now()) },
      )
    }
  }

  @Test
  fun `history window stays inside heartbeat retention`() {
    assertTrue(HISTORY_WINDOW_DURATION < RETENTION_PERIOD_DURATION)
    assertEquals(HISTORY_BUCKET_COUNT.toLong(), HISTORY_WINDOW_DURATION.dividedBy(HISTORY_BUCKET_DURATION))
  }

  /**
   * Check-ins 15 seconds past each half minute, so none lands on a period boundary even after the few
   * milliseconds between the test's clock read and the service's.
   */
  private fun everyThirtySeconds(
    from: OffsetDateTime,
    until: OffsetDateTime,
  ): List<OffsetDateTime> =
    generateSequence(from.plusSeconds(15)) { it.plusSeconds(30) }
      .takeWhile { it.isBefore(until) }
      .toList()

  /** Newest first, the order the repository queries return, so the tests fail if the service stops sorting. */
  private fun heartbeatLogsAt(
    dataplaneId: UUID,
    checkIns: List<OffsetDateTime>,
  ): List<DataplaneHeartbeatLog> =
    checkIns.sortedDescending().map { checkIn ->
      DataplaneHeartbeatLog(
        id = UUID.randomUUID(),
        dataplaneId = dataplaneId,
        controlPlaneVersion = "1.0.0",
        dataplaneVersion = "2.0.0",
        createdAt = checkIn,
      )
    }

  @Test
  fun `cleanupOldHeartbeats calls repository with correct cutoff time`() {
    val deletedCount = 5
    every { heartbeatLogRepository.deleteOldHeartbeatsExceptLatest(any()) } returns deletedCount

    val result = dataplaneHealthService.cleanupOldHeartbeats()

    assertEquals(deletedCount, result)
    verify(exactly = 1) {
      heartbeatLogRepository.deleteOldHeartbeatsExceptLatest(
        match {
          // Verify cutoff time is approximately now minus retention period (within 1 second tolerance)
          val expectedCutoff = OffsetDateTime.now().minus(RETENTION_PERIOD_DURATION)
          it.isAfter(expectedCutoff.minusSeconds(1)) && it.isBefore(expectedCutoff.plusSeconds(1))
        },
      )
    }
  }

  @Test
  fun `cleanupOldHeartbeats returns zero when no records deleted`() {
    every { heartbeatLogRepository.deleteOldHeartbeatsExceptLatest(any()) } returns 0

    val result = dataplaneHealthService.cleanupOldHeartbeats()

    assertEquals(0, result)
    verify(exactly = 1) { heartbeatLogRepository.deleteOldHeartbeatsExceptLatest(any()) }
  }

  @Test
  fun `cleanupOldHeartbeats returns deleted count`() {
    val deletedCount = 42
    every { heartbeatLogRepository.deleteOldHeartbeatsExceptLatest(any()) } returns deletedCount

    val result = dataplaneHealthService.cleanupOldHeartbeats()

    assertEquals(deletedCount, result)
  }
}
