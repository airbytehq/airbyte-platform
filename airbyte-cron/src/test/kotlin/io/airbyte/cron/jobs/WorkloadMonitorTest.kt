package io.airbyte.cron.jobs

import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.airbyte.workload.api.client.generated.WorkloadApi
import io.airbyte.workload.api.client.model.generated.Workload
import io.airbyte.workload.api.client.model.generated.WorkloadListRequest
import io.airbyte.workload.api.client.model.generated.WorkloadListResponse
import io.airbyte.workload.api.client.model.generated.WorkloadStatus
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

class WorkloadMonitorTest {
  val claimTimeout = Duration.of(5, ChronoUnit.SECONDS)
  val heartbeatTimeout = Duration.of(6, ChronoUnit.SECONDS)
  val nonStartedTimeout = Duration.of(7, ChronoUnit.SECONDS)
  val nonSyncTimeout = Duration.of(9, ChronoUnit.MINUTES)
  val syncTimeout = Duration.of(30, ChronoUnit.DAYS)

  lateinit var currentTime: OffsetDateTime
  lateinit var metricClient: MetricClient
  lateinit var workloadApi: WorkloadApi
  lateinit var workloadMonitor: WorkloadMonitor

  @BeforeEach
  fun beforeEach() {
    metricClient =
      mockk<MetricClient>().also {
        every { it.count(any(), any(), *anyVararg()) } returns Unit
      }
    workloadApi = mockk()
    workloadMonitor =
      WorkloadMonitor(
        workloadApi = workloadApi,
        claimTimeout = claimTimeout,
        heartbeatTimeout = heartbeatTimeout,
        nonStartedTimeout = nonStartedTimeout,
        nonSyncWorkloadTimeout = nonSyncTimeout,
        syncWorkloadTimeout = syncTimeout,
        metricClient = metricClient,
        timeProvider = { _: ZoneId -> currentTime },
      )
  }

  @Test
  fun `test cancel not started workloads`() {
    val expiredWorkloads = WorkloadListResponse(workloads = listOf(getWorkload("1"), getWorkload("2"), getWorkload("3")))
    currentTime = OffsetDateTime.now()
    every { workloadApi.workloadList(any()) } returns expiredWorkloads
    every { workloadApi.workloadCancel(any()) } returns Unit andThenThrows ServerException() andThen Unit

    workloadMonitor.cancelNotStartedWorkloads()

    verifyAll {
      workloadApi.workloadList(
        match {
          it.status == listOf(WorkloadStatus.CLAIMED) && it.updatedBefore == currentTime.minus(nonStartedTimeout)
        },
      )
      workloadApi.workloadCancel(match { it.workloadId == "1" })
      workloadApi.workloadCancel(match { it.workloadId == "2" })
      workloadApi.workloadCancel(match { it.workloadId == "3" })
    }
    verify(exactly = 2) {
      metricClient.count(
        OssMetricsRegistry.WORKLOADS_CANCEL,
        1,
        MetricAttribute(MetricTags.CANCELLATION_SOURCE, "workload-monitor-start"),
        MetricAttribute(MetricTags.STATUS, "ok"),
      )
    }
    verify(exactly = 1) {
      metricClient.count(
        OssMetricsRegistry.WORKLOADS_CANCEL,
        1,
        MetricAttribute(MetricTags.CANCELLATION_SOURCE, "workload-monitor-start"),
        MetricAttribute(MetricTags.STATUS, "fail"),
      )
    }
  }

  @Test
  fun `test cancel not claimed workloads`() {
    val expiredWorkloads = WorkloadListResponse(workloads = listOf(getWorkload("a"), getWorkload("b"), getWorkload("c")))
    currentTime = OffsetDateTime.now()
    every { workloadApi.workloadList(any()) } returns expiredWorkloads
    every { workloadApi.workloadCancel(any()) } throws ServerException() andThen Unit andThen Unit

    workloadMonitor.cancelNotClaimedWorkloads()

    verifyAll {
      workloadApi.workloadList(
        match {
          it.status == listOf(WorkloadStatus.PENDING) && it.updatedBefore == currentTime.minus(claimTimeout)
        },
      )
      workloadApi.workloadCancel(match { it.workloadId == "a" })
      workloadApi.workloadCancel(match { it.workloadId == "b" })
      workloadApi.workloadCancel(match { it.workloadId == "c" })
    }
    verify(exactly = 2) {
      metricClient.count(
        OssMetricsRegistry.WORKLOADS_CANCEL,
        1,
        MetricAttribute(MetricTags.CANCELLATION_SOURCE, "workload-monitor-claim"),
        MetricAttribute(MetricTags.STATUS, "ok"),
      )
    }
    verify(exactly = 1) {
      metricClient.count(
        OssMetricsRegistry.WORKLOADS_CANCEL,
        1,
        MetricAttribute(MetricTags.CANCELLATION_SOURCE, "workload-monitor-claim"),
        MetricAttribute(MetricTags.STATUS, "fail"),
      )
    }
  }

  @Test
  fun `test cancel not heartbeating workloads`() {
    val expiredWorkloads = WorkloadListResponse(workloads = listOf(getWorkload("3"), getWorkload("4"), getWorkload("5")))
    currentTime = OffsetDateTime.now()
    every {
      workloadApi.workloadList(
        WorkloadListRequest(
          updatedBefore = currentTime.minus(heartbeatTimeout),
          status = listOf(WorkloadStatus.RUNNING, WorkloadStatus.LAUNCHED),
        ),
      )
    } returns expiredWorkloads
    every { workloadApi.workloadCancel(any()) } returns Unit andThenThrows ServerException() andThen Unit

    workloadMonitor.cancelNotHeartbeatingWorkloads()

    verifyAll {
      workloadApi.workloadList(
        match {
          it.status == listOf(WorkloadStatus.RUNNING, WorkloadStatus.LAUNCHED) && it.updatedBefore == currentTime.minus(heartbeatTimeout)
        },
      )
      workloadApi.workloadCancel(match { it.workloadId == "3" })
      workloadApi.workloadCancel(match { it.workloadId == "4" })
      workloadApi.workloadCancel(match { it.workloadId == "5" })
    }
    verify(exactly = 2) {
      metricClient.count(
        OssMetricsRegistry.WORKLOADS_CANCEL,
        1,
        MetricAttribute(MetricTags.CANCELLATION_SOURCE, "workload-monitor-heartbeat"),
        MetricAttribute(MetricTags.STATUS, "ok"),
      )
    }
    verify(exactly = 1) {
      metricClient.count(
        OssMetricsRegistry.WORKLOADS_CANCEL,
        1,
        MetricAttribute(MetricTags.CANCELLATION_SOURCE, "workload-monitor-heartbeat"),
        MetricAttribute(MetricTags.STATUS, "fail"),
      )
    }
  }

  @Test
  fun `test cancel timeout non sync workload`() {
    val expiredWorkloads = WorkloadListResponse(workloads = listOf(getWorkload("3"), getWorkload("4"), getWorkload("5")))
    currentTime = OffsetDateTime.now()
    every { workloadApi.workloadListOldNonSync(any()) } returns expiredWorkloads
    every { workloadApi.workloadCancel(any()) } returns Unit andThenThrows ServerException() andThen Unit

    workloadMonitor.cancelRunningForTooLongNonSyncWorkloads()

    verifyAll {
      workloadApi.workloadListOldNonSync(
        match {
          it.createdBefore == currentTime.minus(nonSyncTimeout)
        },
      )
      workloadApi.workloadCancel(match { it.workloadId == "3" })
      workloadApi.workloadCancel(match { it.workloadId == "4" })
      workloadApi.workloadCancel(match { it.workloadId == "5" })
    }
    verify(exactly = 2) {
      metricClient.count(
        OssMetricsRegistry.WORKLOADS_CANCEL,
        1,
        MetricAttribute(MetricTags.CANCELLATION_SOURCE, "workload-monitor-non-sync-timeout"),
        MetricAttribute(MetricTags.STATUS, "ok"),
      )
    }
    verify(exactly = 1) {
      metricClient.count(
        OssMetricsRegistry.WORKLOADS_CANCEL,
        1,
        MetricAttribute(MetricTags.CANCELLATION_SOURCE, "workload-monitor-non-sync-timeout"),
        MetricAttribute(MetricTags.STATUS, "fail"),
      )
    }
  }

  @Test
  fun `test cancel timeout sync workload`() {
    val expiredWorkloads = WorkloadListResponse(workloads = listOf(getWorkload("3"), getWorkload("4"), getWorkload("5")))
    currentTime = OffsetDateTime.now()
    every { workloadApi.workloadListOldSync(any()) } returns expiredWorkloads
    every { workloadApi.workloadCancel(any()) } returns Unit andThenThrows ServerException() andThen Unit

    workloadMonitor.cancelRunningForTooLongSyncWorkloads()

    verifyAll {
      workloadApi.workloadListOldSync(
        match {
          it.createdBefore == currentTime.minus(syncTimeout)
        },
      )
      workloadApi.workloadCancel(match { it.workloadId == "3" })
      workloadApi.workloadCancel(match { it.workloadId == "4" })
      workloadApi.workloadCancel(match { it.workloadId == "5" })
    }
    verify(exactly = 2) {
      metricClient.count(
        OssMetricsRegistry.WORKLOADS_CANCEL,
        1,
        MetricAttribute(MetricTags.CANCELLATION_SOURCE, "workload-monitor-sync-timeout"),
        MetricAttribute(MetricTags.STATUS, "ok"),
      )
    }
    verify(exactly = 1) {
      metricClient.count(
        OssMetricsRegistry.WORKLOADS_CANCEL,
        1,
        MetricAttribute(MetricTags.CANCELLATION_SOURCE, "workload-monitor-sync-timeout"),
        MetricAttribute(MetricTags.STATUS, "fail"),
      )
    }
  }

  fun getWorkload(id: String): Workload {
    return mockkClass(Workload::class).also {
      every { it.id } returns id
    }
  }
}
