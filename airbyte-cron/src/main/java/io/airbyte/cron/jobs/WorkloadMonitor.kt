package io.airbyte.cron.jobs

import datadog.trace.api.Trace
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.airbyte.workload.api.client.generated.WorkloadApi
import io.airbyte.workload.api.client.model.generated.Workload
import io.airbyte.workload.api.client.model.generated.WorkloadCancelRequest
import io.airbyte.workload.api.client.model.generated.WorkloadListRequest
import io.airbyte.workload.api.client.model.generated.WorkloadStatus
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.Duration
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.ZoneOffset

private val logger = KotlinLogging.logger { }

@Singleton
@Requires(
  property = "airbyte.workload.monitor.enabled",
  value = "true",
)
class WorkloadMonitor(
  private val workloadApi: WorkloadApi,
  @Property(name = "airbyte.workload.monitor.claim-timeout") private val claimTimeout: Duration,
  @Property(name = "airbyte.workload.monitor.heartbeat-timeout") private val heartbeatTimeout: Duration,
  @Named("replicationNotStartedTimeout") private val nonStartedTimeout: Duration,
  private val metricClient: MetricClient,
  private val timeProvider: (ZoneId) -> OffsetDateTime = OffsetDateTime::now,
) {
  @Trace
  @Scheduled(fixedRate = "\${airbyte.workload.monitor.not-started-check-rate}")
  fun cancelNotStartedWorkloads() {
    logger.info { "Checking for not started workloads." }
    val oldestStartedTime = timeProvider(ZoneOffset.UTC).minusSeconds(nonStartedTimeout.seconds)
    val notStartedWorkloads =
      workloadApi.workloadList(
        WorkloadListRequest(
          status = listOf(WorkloadStatus.CLAIMED),
          updatedBefore = oldestStartedTime,
        ),
      )

    cancelWorkloads(notStartedWorkloads.workloads, "Not started within time limit", "workload-monitor-start")
  }

  @Trace
  @Scheduled(fixedRate = "\${airbyte.workload.monitor.claim-check-rate}")
  fun cancelNotClaimedWorkloads() {
    logger.info { "Checking for not claimed workloads." }
    val oldestClaimTime = timeProvider(ZoneOffset.UTC).minusSeconds(claimTimeout.seconds)
    val notClaimedWorkloads =
      workloadApi.workloadList(
        WorkloadListRequest(
          status = listOf(WorkloadStatus.PENDING),
          updatedBefore = oldestClaimTime,
        ),
      )

    cancelWorkloads(notClaimedWorkloads.workloads, "Not claimed within time limit", "workload-monitor-claim")
  }

  @Trace
  @Scheduled(fixedRate = "\${airbyte.workload.monitor.heartbeat-check-rate}")
  fun cancelNotHeartbeatingWorkloads() {
    logger.info { "Checking for non heartbeating workloads." }
    val oldestHeartbeatTime = timeProvider(ZoneOffset.UTC).minusSeconds(heartbeatTimeout.seconds)
    val nonHeartbeatingWorkloads =
      workloadApi.workloadList(
        WorkloadListRequest(
          status = listOf(WorkloadStatus.RUNNING),
          updatedBefore = oldestHeartbeatTime,
        ),
      )

    cancelWorkloads(nonHeartbeatingWorkloads.workloads, "No heartbeat within time limit", "workload-monitor-heartbeat")
  }

  private fun cancelWorkloads(
    workloads: List<Workload>,
    reason: String,
    source: String,
  ) {
    workloads.map {
      var status = "fail"
      try {
        logger.info { "Cancelling workload ${it.id}, reason: $reason" }
        workloadApi.workloadCancel(WorkloadCancelRequest(workloadId = it.id, reason = reason, source = source))
        status = "ok"
      } catch (e: Exception) {
        logger.warn(e) { "Failed to cancel workload ${it.id}" }
      } finally {
        metricClient.count(
          OssMetricsRegistry.WORKLOADS_CANCEL,
          1,
          MetricAttribute(MetricTags.CANCELLATION_SOURCE, source),
          MetricAttribute(MetricTags.STATUS, status),
        )
      }
    }
  }
}
