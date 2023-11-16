package io.airbyte.cron.jobs

import io.airbyte.workload.api.client2.generated.WorkloadApi
import io.airbyte.workload.api.client2.model.generated.Workload
import io.airbyte.workload.api.client2.model.generated.WorkloadCancelRequest
import io.airbyte.workload.api.client2.model.generated.WorkloadListRequest
import io.airbyte.workload.api.client2.model.generated.WorkloadStatus
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.Duration
import java.time.OffsetDateTime
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
) {
  @Scheduled(fixedRate = "\${airbyte.workload.monitor.not-started-check-rate}")
  fun cancelNotStartedWorkloads() {
    logger.info { "Checking for not started workloads." }
    val oldestStartedTime = OffsetDateTime.now(ZoneOffset.UTC).minusSeconds(nonStartedTimeout.seconds)
    val notStartedWorkloads =
      workloadApi.workloadList(
        WorkloadListRequest(
          status = listOf(WorkloadStatus.CLAIMED),
          updatedBefore = oldestStartedTime,
        ),
      )

    cancelWorkloads(notStartedWorkloads.workloads, "Not started within time limit")
  }

  @Scheduled(fixedRate = "\${airbyte.workload.monitor.claim-check-rate}")
  fun cancelNotClaimedWorkloads() {
    logger.info { "Checking for not claimed workloads." }
    val oldestClaimTime = OffsetDateTime.now(ZoneOffset.UTC).minusSeconds(claimTimeout.seconds)
    val notClaimedWorkloads =
      workloadApi.workloadList(
        WorkloadListRequest(
          status = listOf(WorkloadStatus.PENDING),
          updatedBefore = oldestClaimTime,
        ),
      )

    cancelWorkloads(notClaimedWorkloads.workloads, "Not claimed within time limit")
  }

  @Scheduled(fixedRate = "\${airbyte.workload.monitor.heartbeat-check-rate}")
  fun cancelNotHeartbeatingWorkloads() {
    logger.info { "Checking for non heartbeating workloads." }
    val oldestHeartbeatTime = OffsetDateTime.now(ZoneOffset.UTC).minusSeconds(heartbeatTimeout.seconds)
    val nonHeartbeatingWorkloads =
      workloadApi.workloadList(
        WorkloadListRequest(
          status = listOf(WorkloadStatus.RUNNING),
          updatedBefore = oldestHeartbeatTime,
        ),
      )

    cancelWorkloads(nonHeartbeatingWorkloads.workloads, "No heartbeat within time limit")
  }

  private fun cancelWorkloads(
    workloads: List<Workload>,
    reason: String,
  ) {
    workloads.map {
      try {
        logger.info { "Cancelling workload ${it.id}, reason: $reason" }
        workloadApi.workloadCancel(WorkloadCancelRequest(workloadId = it.id, reason = reason, source = "workload-monitor"))
      } catch (e: Exception) {
        logger.warn(e) { "Failed to cancel workload ${it.id}" }
      }
    }
  }
}
