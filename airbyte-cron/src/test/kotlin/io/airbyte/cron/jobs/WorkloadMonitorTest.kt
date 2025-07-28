/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs

import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.client.generated.WorkloadApi
import io.airbyte.workload.api.client.model.generated.ExpiredDeadlineWorkloadListRequest
import io.airbyte.workload.api.client.model.generated.Workload
import io.airbyte.workload.api.client.model.generated.WorkloadListResponse
import io.airbyte.workload.api.client.model.generated.WorkloadStatus
import io.airbyte.workload.api.client.model.generated.WorkloadType
import io.micrometer.core.instrument.Counter
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkClass
import io.mockk.verify
import io.mockk.verifyAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.openapitools.client.infrastructure.ServerException
import java.time.Duration
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.temporal.ChronoUnit
import java.util.Optional

class WorkloadMonitorTest {
  val nonSyncTimeout = Duration.of(9, ChronoUnit.MINUTES)
  val syncTimeout = Duration.of(30, ChronoUnit.DAYS)

  lateinit var currentTime: OffsetDateTime
  lateinit var metricClient: MetricClient
  lateinit var workloadApi: WorkloadApi
  lateinit var workloadApiClient: WorkloadApiClient
  lateinit var workloadMonitor: WorkloadMonitor
  lateinit var featureFlagClient: FeatureFlagClient

  @BeforeEach
  fun beforeEach() {
    metricClient =
      mockk<MetricClient>().also {
        every { it.count(metric = any(), value = any(), attributes = anyVararg()) } returns mockk<Counter>()
      }
    workloadApi = mockk()
    workloadApiClient = mockk()
    every { workloadApiClient.workloadApi } returns workloadApi
    featureFlagClient = mockk()
    workloadMonitor =
      WorkloadMonitor(
        workloadApiClient = workloadApiClient,
        nonSyncWorkloadTimeout = nonSyncTimeout,
        syncWorkloadTimeout = syncTimeout,
        metricClient = metricClient,
        timeProvider = Optional.of { _: ZoneId -> currentTime },
        deletionBatchSizeLimit = 100,
        featureFlagClient = featureFlagClient,
      )
  }

  @Test
  fun `test cancel not started workloads`() {
    val expiredWorkloads = WorkloadListResponse(workloads = listOf(getWorkload("1"), getWorkload("2"), getWorkload("3")))
    currentTime = OffsetDateTime.now()
    every { workloadApi.workloadListWithExpiredDeadline(any()) } returns expiredWorkloads
    every { workloadApi.workloadFailure(any()) } returns Unit andThenThrows ServerException() andThen Unit

    workloadMonitor.cancelNotStartedWorkloads()

    verifyAll {
      workloadApi.workloadListWithExpiredDeadline(
        match {
          it.status == listOf(WorkloadStatus.CLAIMED) && it.deadline == currentTime
        },
      )
      workloadApi.workloadFailure(match { it.workloadId == "1" })
      workloadApi.workloadFailure(match { it.workloadId == "2" })
      workloadApi.workloadFailure(match { it.workloadId == "3" })
    }
    verify(exactly = 2) {
      metricClient.count(
        OssMetricsRegistry.WORKLOADS_CANCEL,
        1,
        MetricAttribute(MetricTags.CANCELLATION_SOURCE, "workload-monitor-start"),
        MetricAttribute(MetricTags.STATUS, "ok"),
        MetricAttribute(MetricTags.WORKLOAD_TYPE, "sync"),
      )
    }
    verify(exactly = 1) {
      metricClient.count(
        OssMetricsRegistry.WORKLOADS_CANCEL,
        1,
        MetricAttribute(MetricTags.CANCELLATION_SOURCE, "workload-monitor-start"),
        MetricAttribute(MetricTags.STATUS, "fail"),
        MetricAttribute(MetricTags.WORKLOAD_TYPE, "sync"),
      )
    }
  }

  @Test
  fun `test cancel not claimed workloads`() {
    val expiredWorkloads = WorkloadListResponse(workloads = listOf(getWorkload("a"), getWorkload("b"), getWorkload("c")))
    currentTime = OffsetDateTime.now()
    every { workloadApi.workloadListWithExpiredDeadline(any()) } returns expiredWorkloads
    every { workloadApi.workloadFailure(any()) } throws ServerException() andThen Unit andThen Unit

    workloadMonitor.cancelNotClaimedWorkloads()

    verifyAll {
      workloadApi.workloadListWithExpiredDeadline(
        match {
          it.status == listOf(WorkloadStatus.PENDING) && it.deadline == currentTime
        },
      )
      workloadApi.workloadFailure(match { it.workloadId == "a" })
      workloadApi.workloadFailure(match { it.workloadId == "b" })
      workloadApi.workloadFailure(match { it.workloadId == "c" })
    }
    verify(exactly = 2) {
      metricClient.count(
        OssMetricsRegistry.WORKLOADS_CANCEL,
        1,
        MetricAttribute(MetricTags.CANCELLATION_SOURCE, "workload-monitor-claim"),
        MetricAttribute(MetricTags.STATUS, "ok"),
        MetricAttribute(MetricTags.WORKLOAD_TYPE, "sync"),
      )
    }
    verify(exactly = 1) {
      metricClient.count(
        OssMetricsRegistry.WORKLOADS_CANCEL,
        1,
        MetricAttribute(MetricTags.CANCELLATION_SOURCE, "workload-monitor-claim"),
        MetricAttribute(MetricTags.STATUS, "fail"),
        MetricAttribute(MetricTags.WORKLOAD_TYPE, "sync"),
      )
    }
  }

  @Test
  fun `test cancel not heartbeating workloads`() {
    val expiredWorkloads = WorkloadListResponse(workloads = listOf(getWorkload("3"), getWorkload("4"), getWorkload("5")))
    currentTime = OffsetDateTime.now()
    every {
      workloadApi.workloadListWithExpiredDeadline(
        ExpiredDeadlineWorkloadListRequest(
          deadline = currentTime,
          status = listOf(WorkloadStatus.RUNNING, WorkloadStatus.LAUNCHED),
        ),
      )
    } returns expiredWorkloads
    every { workloadApi.workloadFailure(any()) } returns Unit andThenThrows ServerException() andThen Unit

    workloadMonitor.cancelNotHeartbeatingWorkloads()

    verifyAll {
      workloadApi.workloadListWithExpiredDeadline(
        match {
          it.status == listOf(WorkloadStatus.RUNNING, WorkloadStatus.LAUNCHED) && it.deadline == currentTime
        },
      )
      workloadApi.workloadFailure(match { it.workloadId == "3" })
      workloadApi.workloadFailure(match { it.workloadId == "4" })
      workloadApi.workloadFailure(match { it.workloadId == "5" })
    }
    verify(exactly = 2) {
      metricClient.count(
        OssMetricsRegistry.WORKLOADS_CANCEL,
        1,
        MetricAttribute(MetricTags.CANCELLATION_SOURCE, "workload-monitor-heartbeat"),
        MetricAttribute(MetricTags.STATUS, "ok"),
        MetricAttribute(MetricTags.WORKLOAD_TYPE, "sync"),
      )
    }
    verify(exactly = 1) {
      metricClient.count(
        OssMetricsRegistry.WORKLOADS_CANCEL,
        1,
        MetricAttribute(MetricTags.CANCELLATION_SOURCE, "workload-monitor-heartbeat"),
        MetricAttribute(MetricTags.STATUS, "fail"),
        MetricAttribute(MetricTags.WORKLOAD_TYPE, "sync"),
      )
    }
  }

  @Test
  fun `test cancel timeout non sync workload`() {
    val expiredWorkloads = WorkloadListResponse(workloads = listOf(getWorkload("3"), getWorkload("4"), getWorkload("5")))
    currentTime = OffsetDateTime.now()
    every { workloadApi.workloadListOldNonSync(any()) } returns expiredWorkloads
    every { workloadApi.workloadFailure(any()) } returns Unit andThenThrows ServerException() andThen Unit

    workloadMonitor.cancelRunningForTooLongNonSyncWorkloads()

    verifyAll {
      workloadApi.workloadListOldNonSync(
        match {
          it.createdBefore == currentTime.minus(nonSyncTimeout)
        },
      )
      workloadApi.workloadFailure(match { it.workloadId == "3" })
      workloadApi.workloadFailure(match { it.workloadId == "4" })
      workloadApi.workloadFailure(match { it.workloadId == "5" })
    }
    verify(exactly = 2) {
      metricClient.count(
        OssMetricsRegistry.WORKLOADS_CANCEL,
        1,
        MetricAttribute(MetricTags.CANCELLATION_SOURCE, "workload-monitor-non-sync-timeout"),
        MetricAttribute(MetricTags.STATUS, "ok"),
        MetricAttribute(MetricTags.WORKLOAD_TYPE, "sync"),
      )
    }
    verify(exactly = 1) {
      metricClient.count(
        OssMetricsRegistry.WORKLOADS_CANCEL,
        1,
        MetricAttribute(MetricTags.CANCELLATION_SOURCE, "workload-monitor-non-sync-timeout"),
        MetricAttribute(MetricTags.STATUS, "fail"),
        MetricAttribute(MetricTags.WORKLOAD_TYPE, "sync"),
      )
    }
  }

  @Test
  fun `test cancel timeout sync workload`() {
    val expiredWorkloads = WorkloadListResponse(workloads = listOf(getWorkload("3"), getWorkload("4"), getWorkload("5")))
    currentTime = OffsetDateTime.now()
    every { workloadApi.workloadListOldSync(any()) } returns expiredWorkloads
    every { workloadApi.workloadFailure(any()) } returns Unit andThenThrows ServerException() andThen Unit

    workloadMonitor.cancelRunningForTooLongSyncWorkloads()

    verifyAll {
      workloadApi.workloadListOldSync(
        match {
          it.createdBefore == currentTime.minus(syncTimeout)
        },
      )
      workloadApi.workloadFailure(match { it.workloadId == "3" })
      workloadApi.workloadFailure(match { it.workloadId == "4" })
      workloadApi.workloadFailure(match { it.workloadId == "5" })
    }
    verify(exactly = 2) {
      metricClient.count(
        OssMetricsRegistry.WORKLOADS_CANCEL,
        1,
        MetricAttribute(MetricTags.CANCELLATION_SOURCE, "workload-monitor-sync-timeout"),
        MetricAttribute(MetricTags.STATUS, "ok"),
        MetricAttribute(MetricTags.WORKLOAD_TYPE, "sync"),
      )
    }
    verify(exactly = 1) {
      metricClient.count(
        OssMetricsRegistry.WORKLOADS_CANCEL,
        1,
        MetricAttribute(MetricTags.CANCELLATION_SOURCE, "workload-monitor-sync-timeout"),
        MetricAttribute(MetricTags.STATUS, "fail"),
        MetricAttribute(MetricTags.WORKLOAD_TYPE, "sync"),
      )
    }
  }

  fun getWorkload(id: String): Workload =
    mockkClass(Workload::class).also {
      every { it.id } returns id
      every { it.type } returns WorkloadType.SYNC
    }
}
