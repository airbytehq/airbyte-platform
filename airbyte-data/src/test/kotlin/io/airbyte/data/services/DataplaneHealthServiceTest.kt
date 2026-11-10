/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.version.Version
import io.airbyte.data.repositories.AirbyteReleaseRepository
import io.airbyte.data.repositories.DataplaneDiagnosticRepository
import io.airbyte.data.repositories.DataplaneHeartbeatLogRepository
import io.airbyte.data.repositories.entities.AirbyteRelease
import io.airbyte.data.repositories.entities.DataplaneHeartbeatLog
import io.airbyte.data.services.DataplaneHealthService.Companion.HISTORY_BUCKET_COUNT
import io.airbyte.data.services.DataplaneHealthService.Companion.HISTORY_BUCKET_DURATION
import io.airbyte.data.services.DataplaneHealthService.Companion.HISTORY_WINDOW_DURATION
import io.airbyte.data.services.DataplaneHealthService.Companion.RETENTION_PERIOD_DURATION
import io.airbyte.data.services.DataplaneHealthService.Companion.UNKNOWN_VERSION
import io.airbyte.data.services.DataplaneHealthService.Companion.historyWindowStart
import io.airbyte.data.services.DataplaneHealthService.Companion.releaseSupport
import io.airbyte.domain.models.DataplaneDiagnostic
import io.airbyte.domain.models.DataplaneDiagnosticKind
import io.airbyte.domain.models.DataplaneDiagnosticOutcome
import io.airbyte.domain.models.DataplaneHealthInfo.HealthStatus
import io.airbyte.domain.models.DataplaneVersionSupport
import io.airbyte.domain.models.SavedDataplaneDiagnostic
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.time.Duration
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID
import io.airbyte.data.repositories.entities.DataplaneDiagnostic as DataplaneDiagnosticEntity

internal class DataplaneHealthServiceTest {
  private val heartbeatLogRepository = mockk<DataplaneHeartbeatLogRepository>(relaxed = true)
  private val diagnosticRepository = mockk<DataplaneDiagnosticRepository>(relaxed = true)
  private val releaseRepository = mockk<AirbyteReleaseRepository>()
  private val dataplaneHealthService = DataplaneHealthService(heartbeatLogRepository, diagnosticRepository, releaseRepository)

  @Test
  fun `recordDiagnostics saves each result with its status and error`() {
    val dataplaneId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val checkedAt = OffsetDateTime.parse("2026-11-03T10:00:00Z")
    val observation = mapOf("scope_type" to "workspace", "scope_id" to "11111111-1111-1111-1111-111111111111")
    val passed = diagnostic(DataplaneDiagnosticOutcome.Passed, checkedAt, observation)
    val failed = diagnostic(DataplaneDiagnosticOutcome.Failed("ACCESS_DENIED", "Access denied"), checkedAt, observation)
    val unableToComplete = diagnostic(DataplaneDiagnosticOutcome.UnableToComplete("TIMEOUT", "Timed out"), checkedAt, emptyMap())

    dataplaneHealthService.recordDiagnostics(dataplaneId, organizationId, listOf(passed, failed, unableToComplete))

    verify(exactly = 1) {
      diagnosticRepository.upsertLatest(
        dataplaneId,
        organizationId,
        "SECRET_STORAGE",
        passed.targetId,
        "PASSED",
        checkedAt,
        null,
        null,
        Jsons.serialize(observation),
      )
      diagnosticRepository.upsertLatest(
        dataplaneId,
        organizationId,
        "SECRET_STORAGE",
        failed.targetId,
        "FAILED",
        checkedAt,
        "ACCESS_DENIED",
        "Access denied",
        Jsons.serialize(observation),
      )
      diagnosticRepository.upsertLatest(
        dataplaneId,
        organizationId,
        "SECRET_STORAGE",
        unableToComplete.targetId,
        "UNABLE_TO_COMPLETE",
        checkedAt,
        "TIMEOUT",
        "Timed out",
        "{}",
      )
    }
  }

  @Test
  fun `recordDiagnostics saves nothing for an empty list`() {
    dataplaneHealthService.recordDiagnostics(UUID.randomUUID(), UUID.randomUUID(), emptyList())

    verify(exactly = 0) { diagnosticRepository.upsertLatest(any(), any(), any(), any(), any(), any(), any(), any(), any()) }
  }

  @Test
  fun `getDiagnostics returns saved results and skips rows with an unknown kind or status`() {
    val organizationId = UUID.randomUUID()
    val dataplaneIds = listOf(UUID.randomUUID())
    val checkedAt = OffsetDateTime.parse("2026-11-03T10:00:00Z")
    val updatedAt = checkedAt.plusSeconds(30)
    val observation = mapOf("scope_type" to "workspace", "scope_id" to "11111111-1111-1111-1111-111111111111")
    val passedRow = diagnosticRow(dataplaneIds.first(), "SECRET_STORAGE", "PASSED", checkedAt, updatedAt, observation = Jsons.jsonNode(observation))
    val failedRow =
      diagnosticRow(
        dataplaneIds.first(),
        "SECRET_STORAGE",
        "FAILED",
        checkedAt,
        updatedAt,
        errorCode = "ACCESS_DENIED",
        errorMessage = "Access denied",
      )
    val unknownKindRow = diagnosticRow(dataplaneIds.first(), "IMAGE_PULL", "PASSED", checkedAt, updatedAt)
    val unknownStatusRow = diagnosticRow(dataplaneIds.first(), "SECRET_STORAGE", "SKIPPED", checkedAt, updatedAt)
    every { diagnosticRepository.findByOrganizationIdAndDataplaneIdIn(organizationId, dataplaneIds) } returns
      listOf(passedRow, failedRow, unknownKindRow, unknownStatusRow)

    val result = dataplaneHealthService.getDiagnostics(organizationId, dataplaneIds)

    val expected =
      listOf(
        SavedDataplaneDiagnostic(
          dataplaneId = dataplaneIds.first(),
          diagnostic =
            DataplaneDiagnostic(
              DataplaneDiagnosticKind.SECRET_STORAGE,
              passedRow.targetId,
              DataplaneDiagnosticOutcome.Passed,
              checkedAt,
              observation,
            ),
          updatedAt = updatedAt,
        ),
        SavedDataplaneDiagnostic(
          dataplaneId = dataplaneIds.first(),
          diagnostic =
            DataplaneDiagnostic(
              DataplaneDiagnosticKind.SECRET_STORAGE,
              failedRow.targetId,
              DataplaneDiagnosticOutcome.Failed("ACCESS_DENIED", "Access denied"),
              checkedAt,
              emptyMap(),
            ),
          updatedAt = updatedAt,
        ),
      )
    assertEquals(expected, result)
  }

  @Test
  fun `getDiagnostics returns nothing and skips the query for no dataplanes`() {
    assertEquals(emptyList<SavedDataplaneDiagnostic>(), dataplaneHealthService.getDiagnostics(UUID.randomUUID(), emptyList()))

    verify(exactly = 0) { diagnosticRepository.findByOrganizationIdAndDataplaneIdIn(any(), any()) }
  }

  @Test
  fun `unsupportedDiagnosticKinds lists a check when the version is older than its minimum`() {
    assertEquals(listOf(DataplaneDiagnosticKind.SECRET_STORAGE), unsupportedKinds("2.4.9"))
  }

  @ParameterizedTest
  @ValueSource(strings = ["2.5.0", "2.5.1", "2.10.0", "3.0.0"])
  fun `unsupportedDiagnosticKinds leaves out a check when the version is new enough`(version: String) {
    assertEquals(emptyList<DataplaneDiagnosticKind>(), unsupportedKinds(version))
  }

  @ParameterizedTest
  @ValueSource(strings = ["dev", "dev-e7c34d6-1cede71", UNKNOWN_VERSION, "", "2.5", "2.x.0", "1.x.0", "2.4.x"])
  fun `unsupportedDiagnosticKinds leaves out a check when the version can't be compared`(version: String) {
    assertEquals(emptyList<DataplaneDiagnosticKind>(), unsupportedKinds(version))
  }

  @Test
  fun `unsupportedDiagnosticKinds leaves out a check when the version is missing`() {
    assertEquals(emptyList<DataplaneDiagnosticKind>(), unsupportedKinds(null))
  }

  @Test
  fun `unsupportedDiagnosticKinds leaves out a check with a saved result even when the version is older`() {
    val checkedAt = OffsetDateTime.parse("2026-11-03T10:00:00Z")
    val saved = SavedDataplaneDiagnostic(UUID.randomUUID(), diagnostic(DataplaneDiagnosticOutcome.Passed, checkedAt, emptyMap()), checkedAt)

    assertEquals(emptyList<DataplaneDiagnosticKind>(), unsupportedKinds("2.4.9", listOf(saved)))
  }

  @Test
  fun `unsupportedDiagnosticKinds lists nothing while no check has a minimum version`() {
    assertEquals(emptyList<DataplaneDiagnosticKind>(), dataplaneHealthService.unsupportedDiagnosticKinds("0.0.1", emptyList()))
  }

  private fun unsupportedKinds(
    dataplaneVersion: String?,
    savedDiagnostics: List<SavedDataplaneDiagnostic> = emptyList(),
  ) = dataplaneHealthService.unsupportedDiagnosticKinds(
    dataplaneVersion,
    savedDiagnostics,
    mapOf(DataplaneDiagnosticKind.SECRET_STORAGE to Version("2.5.0")),
  )

  private fun diagnosticRow(
    dataplaneId: UUID,
    kind: String,
    status: String,
    checkedAt: OffsetDateTime,
    updatedAt: OffsetDateTime,
    errorCode: String? = null,
    errorMessage: String? = null,
    observation: JsonNode? = null,
  ) = DataplaneDiagnosticEntity(
    id = UUID.randomUUID(),
    dataplaneId = dataplaneId,
    organizationId = UUID.randomUUID(),
    diagnosticKind = kind,
    targetId = UUID.randomUUID(),
    status = status,
    checkedAt = checkedAt,
    errorCode = errorCode,
    errorMessage = errorMessage,
    observation = observation,
    createdAt = checkedAt,
    updatedAt = updatedAt,
  )

  private fun diagnostic(
    outcome: DataplaneDiagnosticOutcome,
    checkedAt: OffsetDateTime,
    observation: Map<String, Any>,
  ) = DataplaneDiagnostic(
    kind = DataplaneDiagnosticKind.SECRET_STORAGE,
    targetId = UUID.randomUUID(),
    outcome = outcome,
    checkedAt = checkedAt,
    observation = observation,
  )

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

  @Test
  fun `releaseSupport keeps a release supported until 14 calendar months after publication`() {
    val publishedAt = OffsetDateTime.of(2025, 9, 15, 12, 0, 0, 0, ZoneOffset.UTC)
    val supportedUntil = OffsetDateTime.of(2026, 11, 15, 12, 0, 0, 0, ZoneOffset.UTC)

    assertEquals(
      DataplaneVersionSupport.Published(publishedAt = publishedAt, supportedUntil = supportedUntil, supported = true),
      releaseSupport(publishedAt, now = supportedUntil.minusNanos(1)),
    )
    assertEquals(
      DataplaneVersionSupport.Published(publishedAt = publishedAt, supportedUntil = supportedUntil, supported = false),
      releaseSupport(publishedAt, now = supportedUntil),
    )
  }

  @Test
  fun `releaseSupport ends on the last day of a shorter month`() {
    val publishedAt = OffsetDateTime.of(2024, 12, 31, 8, 0, 0, 0, ZoneOffset.UTC)

    assertEquals(OffsetDateTime.of(2026, 2, 28, 8, 0, 0, 0, ZoneOffset.UTC), releaseSupport(publishedAt, publishedAt).supportedUntil)
  }

  @Test
  fun `releaseSupport counts months on the UTC calendar`() {
    // 2025-01-31T23:00 at -05:00 is 2025-02-01T04:00 UTC, so the end is April 1st, not March 31st.
    val publishedAt = OffsetDateTime.of(2025, 1, 31, 23, 0, 0, 0, ZoneOffset.ofHours(-5))

    assertEquals(
      DataplaneVersionSupport.Published(
        publishedAt = OffsetDateTime.of(2025, 2, 1, 4, 0, 0, 0, ZoneOffset.UTC),
        supportedUntil = OffsetDateTime.of(2026, 4, 1, 4, 0, 0, 0, ZoneOffset.UTC),
        supported = true,
      ),
      releaseSupport(publishedAt, publishedAt),
    )
  }

  @Test
  fun `getPublishedReleaseSupport evaluates each published release and leaves out versions the catalog does not list`() {
    val publishedAt = OffsetDateTime.of(2026, 1, 10, 0, 0, 0, 0, ZoneOffset.UTC)
    val now = OffsetDateTime.of(2026, 11, 6, 0, 0, 0, 0, ZoneOffset.UTC)
    val reportedVersions = setOf("2.4.0", UNKNOWN_VERSION, "2.4.0-custom", "dev-1a2b3c4d5e")
    every { releaseRepository.findByVersionIn(reportedVersions) } returns listOf(release("2.4.0", publishedAt))

    val result = dataplaneHealthService.getPublishedReleaseSupport(reportedVersions, now)

    assertEquals(mapOf("2.4.0" to releaseSupport(publishedAt, now)), result)
  }

  @Test
  fun `getPublishedReleaseSupport ages a release out between reads without a new check-in`() {
    val publishedAt = OffsetDateTime.of(2025, 9, 1, 0, 0, 0, 0, ZoneOffset.UTC)
    every { releaseRepository.findByVersionIn(setOf("2.4.0")) } returns listOf(release("2.4.0", publishedAt))

    val beforeEnd = dataplaneHealthService.getPublishedReleaseSupport(setOf("2.4.0"), OffsetDateTime.of(2026, 10, 31, 23, 59, 0, 0, ZoneOffset.UTC))
    val afterEnd = dataplaneHealthService.getPublishedReleaseSupport(setOf("2.4.0"), OffsetDateTime.of(2026, 11, 1, 0, 0, 0, 0, ZoneOffset.UTC))

    assertEquals(listOf(true), beforeEnd.values.map { it.supported }, "$beforeEnd")
    assertEquals(listOf(false), afterEnd.values.map { it.supported }, "$afterEnd")
    verify(exactly = 0) { heartbeatLogRepository.save(any()) }
  }

  @Test
  fun `getPublishedReleaseSupport returns nothing for an empty catalog`() {
    every { releaseRepository.findByVersionIn(setOf("2.4.0")) } returns emptyList()

    assertEquals(emptyMap<String, DataplaneVersionSupport.Published>(), dataplaneHealthService.getPublishedReleaseSupport(setOf("2.4.0")))
  }

  @Test
  fun `getPublishedReleaseSupport does not query the catalog without reported versions`() {
    assertEquals(emptyMap<String, DataplaneVersionSupport.Published>(), dataplaneHealthService.getPublishedReleaseSupport(emptySet()))

    verify(exactly = 0) { releaseRepository.findByVersionIn(any()) }
  }

  private fun release(
    version: String,
    publishedAt: OffsetDateTime,
  ) = AirbyteRelease(
    id = UUID.randomUUID(),
    version = version,
    sourceCommit = "0123456789abcdef0123456789abcdef01234567",
    publishedAt = publishedAt,
  )
}
