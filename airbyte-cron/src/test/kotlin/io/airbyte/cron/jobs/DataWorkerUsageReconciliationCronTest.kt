/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs

import io.airbyte.data.repositories.DataWorkerUsageReservationCandidate
import io.airbyte.domain.services.dataworker.DataWorkerUsageService
import io.airbyte.featureflag.DataWorkerUsageReconciliationMode
import io.airbyte.featureflag.Empty
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.Optional
import java.util.UUID

class DataWorkerUsageReconciliationCronTest {
  private val now = OffsetDateTime.of(2026, 8, 26, 12, 30, 0, 0, ZoneOffset.UTC)

  private lateinit var dataWorkerUsageService: DataWorkerUsageService
  private lateinit var featureFlagClient: FeatureFlagClient
  private lateinit var metricClient: MetricClient
  private lateinit var cron: DataWorkerUsageReconciliationCron
  private var requestedZone: ZoneId? = null

  @BeforeEach
  fun setup() {
    dataWorkerUsageService = mockk()
    featureFlagClient = mockk()
    metricClient = mockk(relaxed = true)
    requestedZone = null
    cron =
      DataWorkerUsageReconciliationCron(
        dataWorkerUsageService = dataWorkerUsageService,
        featureFlagClient = featureFlagClient,
        metricClient = metricClient,
        timeProvider =
          Optional.of { zone ->
            requestedZone = zone
            now
          },
      )

    every { featureFlagClient.stringVariation(DataWorkerUsageReconciliationMode, Empty) } returns "enabled"
    every { dataWorkerUsageService.findTerminalReservationCandidates(any(), any()) } returns emptyList()
  }

  @Test
  fun `disabled reconciliation does no database work`() {
    every { featureFlagClient.stringVariation(DataWorkerUsageReconciliationMode, Empty) } returns "disabled"

    cron.reconcileDataWorkerUsage()

    verify(exactly = 0) { dataWorkerUsageService.findTerminalReservationCandidates(any(), any()) }
    verify(exactly = 0) { dataWorkerUsageService.releaseReservedUsageForJob(any(), any()) }
    verifyRunMetrics("disabled")
  }

  @Test
  fun `dry-run discovers with the UTC grace-period cutoff without releasing candidates`() {
    val candidates = listOf(candidate(jobId = 1), candidate(jobId = 2))
    every { featureFlagClient.stringVariation(DataWorkerUsageReconciliationMode, Empty) } returns "dry_run"
    every { dataWorkerUsageService.findTerminalReservationCandidates(any(), any()) } returns candidates

    cron.reconcileDataWorkerUsage()

    assertEquals(ZoneOffset.UTC, requestedZone)
    verify(exactly = 1) { dataWorkerUsageService.findTerminalReservationCandidates(now.minusMinutes(15), null) }
    verify(exactly = 0) { dataWorkerUsageService.releaseReservedUsageForJob(any(), any()) }
    verifyRunMetrics("success", candidateCount = candidates.size, dryRun = true)
  }

  @Test
  fun `unsupported mode fails closed without database work`() {
    every { featureFlagClient.stringVariation(DataWorkerUsageReconciliationMode, Empty) } returns "unsupported"

    cron.reconcileDataWorkerUsage()

    verify(exactly = 0) { dataWorkerUsageService.findTerminalReservationCandidates(any(), any()) }
    verify(exactly = 0) { dataWorkerUsageService.releaseReservedUsageForJob(any(), any()) }
    verifyRunMetrics("flag_error")
  }

  @Test
  fun `flag evaluation failure does no database work`() {
    every { featureFlagClient.stringVariation(DataWorkerUsageReconciliationMode, Empty) } throws
      RuntimeException("flag service unavailable")

    cron.reconcileDataWorkerUsage()

    verify(exactly = 0) { dataWorkerUsageService.findTerminalReservationCandidates(any(), any()) }
    verify(exactly = 0) { dataWorkerUsageService.releaseReservedUsageForJob(any(), any()) }
    verifyRunMetrics("flag_error")
  }

  @Test
  fun `enabled reconciliation discovers with the UTC grace-period cutoff and succeeds when empty`() {
    cron.reconcileDataWorkerUsage()

    assertEquals(ZoneOffset.UTC, requestedZone)
    verify(exactly = 1) { dataWorkerUsageService.findTerminalReservationCandidates(now.minusMinutes(15), null) }
    verify(exactly = 0) { dataWorkerUsageService.releaseReservedUsageForJob(any(), any()) }
    verifyRunMetrics("success", candidateCount = 0)
  }

  @Test
  fun `successful and already-released candidates complete successfully`() {
    val released = candidate(jobId = 1)
    val alreadyReleased = candidate(jobId = 2)
    every { dataWorkerUsageService.findTerminalReservationCandidates(any(), any()) } returns listOf(released, alreadyReleased)
    every { dataWorkerUsageService.releaseReservedUsageForJob(released.jobId, released.organizationId) } returns true
    every { dataWorkerUsageService.releaseReservedUsageForJob(alreadyReleased.jobId, alreadyReleased.organizationId) } returns false

    cron.reconcileDataWorkerUsage()

    verify(exactly = 1) { dataWorkerUsageService.releaseReservedUsageForJob(released.jobId, released.organizationId) }
    verify(exactly = 1) { dataWorkerUsageService.releaseReservedUsageForJob(alreadyReleased.jobId, alreadyReleased.organizationId) }
    verifyRunMetrics("success", candidateCount = 2)
  }

  @Test
  fun `failed candidate does not prevent later candidates and produces a partial failure`() {
    val released = candidate(jobId = 1)
    val failed = candidate(jobId = 2)
    val alreadyReleased = candidate(jobId = 3)
    every { dataWorkerUsageService.findTerminalReservationCandidates(any(), any()) } returns listOf(released, failed, alreadyReleased)
    every { dataWorkerUsageService.releaseReservedUsageForJob(released.jobId, released.organizationId) } returns true
    every { dataWorkerUsageService.releaseReservedUsageForJob(failed.jobId, failed.organizationId) } throws
      RuntimeException("release failed")
    every { dataWorkerUsageService.releaseReservedUsageForJob(alreadyReleased.jobId, alreadyReleased.organizationId) } returns false

    cron.reconcileDataWorkerUsage()

    verifyOrder {
      dataWorkerUsageService.releaseReservedUsageForJob(released.jobId, released.organizationId)
      dataWorkerUsageService.releaseReservedUsageForJob(failed.jobId, failed.organizationId)
      dataWorkerUsageService.releaseReservedUsageForJob(alreadyReleased.jobId, alreadyReleased.organizationId)
    }
    verifyRunMetrics("partial_failure", candidateCount = 3)
  }

  @Test
  fun `saturated failing batch advances to later candidates on the next run`() {
    val failingCandidates =
      (1L..DataWorkerUsageService.RECONCILIATION_BATCH_SIZE.toLong()).map { candidate(jobId = it) }
    val laterCandidate = candidate(jobId = DataWorkerUsageService.RECONCILIATION_BATCH_SIZE + 1L)
    every {
      dataWorkerUsageService.findTerminalReservationCandidates(any(), any())
    } returnsMany listOf(failingCandidates, listOf(laterCandidate))
    failingCandidates.forEach { candidate ->
      every {
        dataWorkerUsageService.releaseReservedUsageForJob(candidate.jobId, candidate.organizationId)
      } throws RuntimeException("release failed")
    }
    every {
      dataWorkerUsageService.releaseReservedUsageForJob(laterCandidate.jobId, laterCandidate.organizationId)
    } returns true

    cron.reconcileDataWorkerUsage()
    cron.reconcileDataWorkerUsage()

    verifyOrder {
      dataWorkerUsageService.findTerminalReservationCandidates(now.minusMinutes(15), null)
      failingCandidates.forEach { candidate ->
        dataWorkerUsageService.releaseReservedUsageForJob(candidate.jobId, candidate.organizationId)
      }
      dataWorkerUsageService.findTerminalReservationCandidates(now.minusMinutes(15), failingCandidates.last())
      dataWorkerUsageService.releaseReservedUsageForJob(laterCandidate.jobId, laterCandidate.organizationId)
    }
  }

  @Test
  fun `mode change resets the cursor before discovery`() {
    val fullBatch =
      (1L..DataWorkerUsageService.RECONCILIATION_BATCH_SIZE.toLong()).map { candidate(jobId = it) }
    every {
      featureFlagClient.stringVariation(DataWorkerUsageReconciliationMode, Empty)
    } returnsMany listOf("dry_run", "enabled")
    every {
      dataWorkerUsageService.findTerminalReservationCandidates(any(), any())
    } returnsMany listOf(fullBatch, emptyList())

    cron.reconcileDataWorkerUsage()
    cron.reconcileDataWorkerUsage()

    verifyOrder {
      dataWorkerUsageService.findTerminalReservationCandidates(now.minusMinutes(15), null)
      dataWorkerUsageService.findTerminalReservationCandidates(now.minusMinutes(15), null)
    }
    verify(exactly = 0) { dataWorkerUsageService.releaseReservedUsageForJob(any(), any()) }
  }

  @Test
  fun `empty page after a saturated batch resets the cursor`() {
    val fullBatch =
      (1L..DataWorkerUsageService.RECONCILIATION_BATCH_SIZE.toLong()).map { candidate(jobId = it) }
    every {
      dataWorkerUsageService.findTerminalReservationCandidates(any(), any())
    } returnsMany listOf(fullBatch, emptyList(), emptyList())
    every { dataWorkerUsageService.releaseReservedUsageForJob(any(), any()) } returns true

    cron.reconcileDataWorkerUsage()
    cron.reconcileDataWorkerUsage()
    cron.reconcileDataWorkerUsage()

    verifyOrder {
      dataWorkerUsageService.findTerminalReservationCandidates(now.minusMinutes(15), null)
      fullBatch.forEach { candidate ->
        dataWorkerUsageService.releaseReservedUsageForJob(candidate.jobId, candidate.organizationId)
      }
      dataWorkerUsageService.findTerminalReservationCandidates(now.minusMinutes(15), fullBatch.last())
      dataWorkerUsageService.findTerminalReservationCandidates(now.minusMinutes(15), null)
    }
  }

  @Test
  fun `discovery failure records failure and propagates`() {
    val failure = RuntimeException("database unavailable")
    every { dataWorkerUsageService.findTerminalReservationCandidates(any(), any()) } throws failure

    val thrown =
      assertThrows<RuntimeException> {
        cron.reconcileDataWorkerUsage()
      }

    assertSame(failure, thrown)
    verify(exactly = 0) { dataWorkerUsageService.releaseReservedUsageForJob(any(), any()) }
    verifyRunMetrics("failure")
  }

  @Test
  fun `dry-run discovery failure records dry-run failure and propagates`() {
    val failure = RuntimeException("database unavailable")
    every { featureFlagClient.stringVariation(DataWorkerUsageReconciliationMode, Empty) } returns "dry_run"
    every { dataWorkerUsageService.findTerminalReservationCandidates(any(), any()) } throws failure

    val thrown =
      assertThrows<RuntimeException> {
        cron.reconcileDataWorkerUsage()
      }

    assertSame(failure, thrown)
    verify(exactly = 0) { dataWorkerUsageService.releaseReservedUsageForJob(any(), any()) }
    verifyRunMetrics("failure", dryRun = true)
  }

  private fun candidate(jobId: Long): DataWorkerUsageReservationCandidate =
    DataWorkerUsageReservationCandidate(
      jobId = jobId,
      organizationId = UUID.nameUUIDFromBytes("organization-$jobId".toByteArray()),
      terminalAt = now.minusHours(1),
    )

  private fun verifyRunMetrics(
    outcome: String,
    candidateCount: Int? = null,
    dryRun: Boolean = false,
  ) {
    verify(exactly = 1) {
      metricClient.count(
        metric = OssMetricsRegistry.CRON_JOB_RUN_BY_CRON_TYPE,
        value = 1L,
        attributes = arrayOf(MetricAttribute(MetricTags.CRON_TYPE, "data_worker_usage_reconciliation")),
      )
    }
    verify(exactly = 1) {
      metricClient.count(
        metric = OssMetricsRegistry.DATA_WORKER_USAGE_RECONCILIATION_RUN,
        value = 1L,
        attributes =
          arrayOf(
            MetricAttribute(MetricTags.STATUS, outcome),
            MetricAttribute(MetricTags.DRY_RUN, dryRun.toString()),
          ),
      )
    }
    verify(exactly = 2) {
      metricClient.count(metric = any(), value = any(), attributes = anyVararg())
    }
    if (candidateCount == null) {
      verify(exactly = 0) {
        metricClient.gauge(metric = any(), value = any(), attributes = anyVararg())
      }
    } else {
      verify(exactly = 1) {
        metricClient.gauge(
          metric = OssMetricsRegistry.DATA_WORKER_USAGE_RECONCILIATION_CANDIDATE_COUNT,
          value = candidateCount.toDouble(),
          attributes = arrayOf(MetricAttribute(MetricTags.DRY_RUN, dryRun.toString())),
        )
      }
      verify(exactly = 1) {
        metricClient.gauge(metric = any(), value = any(), attributes = anyVararg())
      }
    }
  }
}
