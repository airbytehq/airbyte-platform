/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.data.repositories.DataplaneHeartbeatLogRepository
import io.airbyte.data.repositories.entities.DataplaneHeartbeatLog
import io.airbyte.data.services.DataplaneHealthService.Companion.RETENTION_PERIOD_DURATION
import io.airbyte.data.services.DataplaneHealthService.Companion.UNKNOWN_VERSION
import io.airbyte.domain.models.DataplaneHealthInfo.HealthStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
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

    val healthStatuses = dataplaneHealthService.getDataplaneHealthInfos(listOf(dataplane1, dataplane2, dataplane3))

    assertEquals(3, healthStatuses.size)
    assertEquals(HealthStatus.HEALTHY, healthStatuses[0].status)
    assertEquals(HealthStatus.UNHEALTHY, healthStatuses[1].status)
    assertEquals(HealthStatus.UNKNOWN, healthStatuses[2].status)
  }

  @Test
  fun `getDataplaneHealthStatuses returns empty list for empty input`() {
    val healthStatuses = dataplaneHealthService.getDataplaneHealthInfos(emptyList())

    assertEquals(0, healthStatuses.size)
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
