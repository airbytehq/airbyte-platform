/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher

import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.featureflag.Dataplane
import io.airbyte.featureflag.DeleteRunawayWorkloadsCron
import io.airbyte.featureflag.DetectRunawayWorkloadsCron
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.annotations.Instrument
import io.airbyte.metrics.annotations.Tag
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.domain.WorkloadListActiveRequest
import io.airbyte.workload.api.domain.WorkloadSummary
import io.airbyte.workload.launcher.client.KubernetesClientWrapper
import io.airbyte.workload.launcher.model.DataplaneConfig
import io.airbyte.workload.launcher.pods.AUTO_ID
import io.fabric8.kubernetes.api.model.Pod
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Singleton
import java.time.Clock
import java.util.UUID
import kotlin.time.Duration.Companion.days
import kotlin.time.toJavaDuration

private val logger = KotlinLogging.logger {}

// this is open to support @Instrument AOP
@Singleton
open class RunawayPodSweeper(
  private val workloadApi: WorkloadApiClient,
  private val k8sWrapper: KubernetesClientWrapper,
  private val metricClient: MetricClient,
  private val clock: Clock,
  private val featureFlagClient: FeatureFlagClient,
  @Value("\${airbyte.worker.job.kube.namespace}") private val namespace: String,
) : ApplicationEventListener<DataplaneConfig> {
  private var dataplaneId: UUID? = null

  override fun onApplicationEvent(event: DataplaneConfig?) {
    dataplaneId = event?.dataplaneId
  }

  @Instrument(
    start = "WORKLOAD_LAUNCHER_CRON",
    duration = "WORKLOAD_LAUNCHER_CRON_DURATION",
    tags = [Tag(key = MetricTags.CRON_TYPE, value = "runaway_pod_mark")],
  )
  // At minute 35 of every hour
  @Scheduled(cron = "35 * * * *")
  open fun mark() {
    dataplaneId?.let {
      if (featureFlagClient.boolVariation(DetectRunawayWorkloadsCron, Dataplane(it.toString()))) {
        mark(it)
      }
    }
  }

  @InternalForTesting
  internal fun mark(dataplaneId: UUID) {
    logger.info { "Marking runaway pods" }

    val activeWorkloadsByAutoId: Map<String, WorkloadSummary> =
      workloadApi
        .workloadListActive(WorkloadListActiveRequest(listOf(dataplaneId.toString())))
        .workloads
        .associateBy { it.autoId }

    // Note that we include the pods that have already been marked in previous runs.
    // This is to report overall counts of runaway pods rather than just the new ones.
    val activePodsByAutoId: Map<String, Pod> =
      k8sWrapper
        .listJobPods(namespace)
        .items
        .filter { it.status.phase == "Running" }
        .mapNotNull { pod ->
          pod.metadata.labels[AUTO_ID]?.let { autoId -> autoId to pod }
        }.toMap()

    val autoIdsToMark = activePodsByAutoId - activeWorkloadsByAutoId.keys
    metricClient.count(OssMetricsRegistry.WORKLOAD_RUNAWAY_POD, autoIdsToMark.size.toLong())

    val deleteBy =
      clock
        .instant()
        .plus(DELETION_GRACE_PERIOD)
        .epochSecond
        .toString()

    var markedPods = 0
    autoIdsToMark.values
      .filter { it.metadata.labels[DELETE_BY] == null }
      .forEach { pod ->
        logger.info { "Marking pod ${pod.metadata.name} for deletion by $deleteBy" }
        k8sWrapper.addLabelsToPod(pod, mapOf(DELETE_BY to deleteBy))
        markedPods++
      }

    logger.info {
      "Mark summary: active_workloads:${activeWorkloadsByAutoId.size}, job_pods:${activePodsByAutoId.size}, " +
        "runaway:${autoIdsToMark.size} marked_pods:$markedPods"
    }
  }

  @Instrument(
    start = "WORKLOAD_LAUNCHER_CRON",
    duration = "WORKLOAD_LAUNCHER_CRON_DURATION",
    tags = [Tag(key = MetricTags.CRON_TYPE, value = "runaway_pod_sweep")],
  )
  // At minute 40 of every hour
  @Scheduled(cron = "40 * * * *")
  open fun sweep() {
    dataplaneId?.let { sweep(it) }
  }

  @InternalForTesting
  internal fun sweep(dataplaneId: UUID) {
    val context = Dataplane(dataplaneId.toString())
    if (!featureFlagClient.boolVariation(DetectRunawayWorkloadsCron, context)) {
      return
    }
    val shouldDelete = featureFlagClient.boolVariation(DeleteRunawayWorkloadsCron, context)

    logger.info { "Sweeping for runaway pods" }

    val currentEpochSeconds = clock.instant().epochSecond

    k8sWrapper
      .listJobPods(namespace) { it.withLabel(DELETE_BY) }
      .items
      .forEach { pod ->
        val deleteBy = pod.metadata.labels[DELETE_BY]?.toLongOrNull()
        if (deleteBy != null && deleteBy <= currentEpochSeconds) {
          if (shouldDelete) {
            if (k8sWrapper.deletePod(pod = pod, namespace = namespace, reason = "Runaway pod")) {
              metricClient.count(metric = OssMetricsRegistry.WORKLOAD_RUNAWAY_POD_DELETED, value = 1)
            } else {
              logger.info { "RunawaySweeperDryRun: would have deleted pod ${pod.metadata.name}" }
              metricClient.count(metric = OssMetricsRegistry.WORKLOAD_RUNAWAY_POD_DELETED, value = 1)
            }
          }
        }
      }
  }

  companion object {
    const val DELETE_BY = "airbyte/delete-by"
    val DELETION_GRACE_PERIOD = 1.days.toJavaDuration()
  }
}
