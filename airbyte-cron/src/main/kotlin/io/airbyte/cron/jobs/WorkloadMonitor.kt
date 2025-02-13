/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs

import datadog.trace.api.Trace
import io.airbyte.metrics.annotations.Instrument
import io.airbyte.metrics.annotations.Tag
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.client.model.generated.ExpiredDeadlineWorkloadListRequest
import io.airbyte.workload.api.client.model.generated.LongRunningWorkloadRequest
import io.airbyte.workload.api.client.model.generated.Workload
import io.airbyte.workload.api.client.model.generated.WorkloadFailureRequest
import io.airbyte.workload.api.client.model.generated.WorkloadStatus
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Property
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Singleton
import java.time.Duration
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.Optional
import kotlin.jvm.optionals.getOrElse

private val logger = KotlinLogging.logger { }

@Singleton
open class WorkloadMonitor(
  private val workloadApiClient: WorkloadApiClient,
  @Property(name = "airbyte.workload.monitor.non-sync-workload-timeout") private val nonSyncWorkloadTimeout: Duration,
  @Property(name = "airbyte.workload.monitor.sync-workload-timeout") private val syncWorkloadTimeout: Duration,
  private val metricClient: MetricClient,
  private val timeProvider: Optional<(ZoneId) -> OffsetDateTime>,
) {
  companion object {
    const val CHECK_CLAIMS = "workload-monitor-claim"
    const val CHECK_HEARTBEAT = "workload-monitor-heartbeat"
    const val CHECK_NON_SYNC_TIMEOUT = "workload-monitor-non-sync-timeout"
    const val CHECK_START = "workload-monitor-start"
    const val CHECK_SYNC_TIMEOUT = "workload-monitor-sync-timeout"
    val DEFAULT_TIME_PROVIDER: (ZoneId) -> OffsetDateTime = OffsetDateTime::now
  }

  @Trace
  @Instrument(
    start = "WORKLOAD_MONITOR_RUN",
    end = "WORKLOAD_MONITOR_DONE",
    duration = "WORKLOAD_MONITOR_DURATION",
    tags = [Tag(key = MetricTags.CRON_TYPE, value = CHECK_START)],
  )
  @Scheduled(fixedRate = "\${airbyte.workload.monitor.not-started-check-rate}")
  open fun cancelNotStartedWorkloads() {
    logger.info { "Checking for not started workloads." }
    val oldestStartedTime = timeProvider.getOrElse { DEFAULT_TIME_PROVIDER }.invoke(ZoneOffset.UTC)
    val notStartedWorkloads =
      workloadApiClient.workloadApi.workloadListWithExpiredDeadline(
        ExpiredDeadlineWorkloadListRequest(
          oldestStartedTime,
          status = listOf(WorkloadStatus.CLAIMED),
        ),
      )
    failWorkloads(
      notStartedWorkloads.workloads,
      "Airbyte could not start the process within time limit. The workload was claimed but never started.",
      CHECK_START,
    )
  }

  @Trace
  @Instrument(
    start = "WORKLOAD_MONITOR_RUN",
    end = "WORKLOAD_MONITOR_DONE",
    duration = "WORKLOAD_MONITOR_DURATION",
    tags = [Tag(key = MetricTags.CRON_TYPE, value = CHECK_CLAIMS)],
  )
  @Scheduled(fixedRate = "\${airbyte.workload.monitor.claim-check-rate}")
  open fun cancelNotClaimedWorkloads() {
    logger.info { "Checking for not claimed workloads." }
    val oldestClaimTime = timeProvider.getOrElse { DEFAULT_TIME_PROVIDER }.invoke(ZoneOffset.UTC)
    val notClaimedWorkloads =
      workloadApiClient.workloadApi.workloadListWithExpiredDeadline(
        ExpiredDeadlineWorkloadListRequest(
          oldestClaimTime,
          status = listOf(WorkloadStatus.PENDING),
        ),
      )

    failWorkloads(
      notClaimedWorkloads.workloads,
      "Airbyte could not start the process within time limit. The workload was never claimed.",
      CHECK_CLAIMS,
    )
  }

  @Trace
  @Instrument(
    start = "WORKLOAD_MONITOR_RUN",
    end = "WORKLOAD_MONITOR_DONE",
    duration = "WORKLOAD_MONITOR_DURATION",
    tags = [Tag(key = MetricTags.CRON_TYPE, value = CHECK_HEARTBEAT)],
  )
  @Scheduled(fixedRate = "\${airbyte.workload.monitor.heartbeat-check-rate}")
  open fun cancelNotHeartbeatingWorkloads() {
    logger.info { "Checking for non heartbeating workloads." }
    val oldestHeartbeatTime = timeProvider.getOrElse { DEFAULT_TIME_PROVIDER }.invoke(ZoneOffset.UTC)
    val nonHeartbeatingWorkloads =
      workloadApiClient.workloadApi.workloadListWithExpiredDeadline(
        ExpiredDeadlineWorkloadListRequest(
          oldestHeartbeatTime,
          status = listOf(WorkloadStatus.RUNNING, WorkloadStatus.LAUNCHED),
        ),
      )

    failWorkloads(
      nonHeartbeatingWorkloads.workloads,
      "Airbyte could not track the sync progress. " +
        "Sync process exited without reporting status.",
      CHECK_HEARTBEAT,
    )
  }

  @Trace
  @Instrument(
    start = "WORKLOAD_MONITOR_RUN",
    end = "WORKLOAD_MONITOR_DONE",
    duration = "WORKLOAD_MONITOR_DURATION",
    tags = [Tag(key = MetricTags.CRON_TYPE, value = CHECK_NON_SYNC_TIMEOUT)],
  )
  @Scheduled(fixedRate = "\${airbyte.workload.monitor.non-sync-age-check-rate}")
  open fun cancelRunningForTooLongNonSyncWorkloads() {
    logger.info { "Checking for workloads running for too long with timeout value $nonSyncWorkloadTimeout" }
    val nonHeartbeatingWorkloads =
      workloadApiClient.workloadApi.workloadListOldNonSync(
        LongRunningWorkloadRequest(
          createdBefore = timeProvider.getOrElse { DEFAULT_TIME_PROVIDER }.invoke(ZoneOffset.UTC).minus(nonSyncWorkloadTimeout),
        ),
      )

    failWorkloads(nonHeartbeatingWorkloads.workloads, "Non sync workload timeout", CHECK_NON_SYNC_TIMEOUT)
  }

  @Trace
  @Instrument(
    start = "WORKLOAD_MONITOR_RUN",
    end = "WORKLOAD_MONITOR_DONE",
    duration = "WORKLOAD_MONITOR_DURATION",
    tags = [Tag(key = MetricTags.CRON_TYPE, value = CHECK_SYNC_TIMEOUT)],
  )
  @Scheduled(fixedRate = "\${airbyte.workload.monitor.sync-age-check-rate}")
  open fun cancelRunningForTooLongSyncWorkloads() {
    logger.info { "Checking for sync workloads running for too long with timeout value $syncWorkloadTimeout" }
    val nonHeartbeatingWorkloads =
      workloadApiClient.workloadApi.workloadListOldSync(
        LongRunningWorkloadRequest(
          createdBefore = timeProvider.getOrElse { DEFAULT_TIME_PROVIDER }.invoke(ZoneOffset.UTC).minus(syncWorkloadTimeout),
        ),
      )

    failWorkloads(nonHeartbeatingWorkloads.workloads, "Sync workload timeout", CHECK_SYNC_TIMEOUT)
  }

  private fun failWorkloads(
    workloads: List<Workload>,
    reason: String,
    source: String,
  ) {
    workloads.map {
      var status = "fail"
      try {
        logger.info { "Cancelling workload ${it.id}, reason: $reason" }
        workloadApiClient.workloadApi.workloadFailure(
          WorkloadFailureRequest(
            workloadId = it.id,
            reason = reason,
            source = source,
          ),
        )
        status = "ok"
      } catch (e: Exception) {
        logger.warn(e) { "Failed to cancel workload ${it.id}" }
      } finally {
        metricClient.count(
          OssMetricsRegistry.WORKLOADS_CANCEL,
          1,
          MetricAttribute(MetricTags.CANCELLATION_SOURCE, source),
          MetricAttribute(MetricTags.STATUS, status),
          MetricAttribute(MetricTags.WORKLOAD_TYPE, it.type.value),
        )
      }
    }
  }
}
