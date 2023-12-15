package io.airbyte.workload.launcher.pipeline.stages

import datadog.trace.api.Trace
import io.airbyte.metrics.annotations.Instrument
import io.airbyte.metrics.annotations.Tag
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.metrics.MeterFilterFactory
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.MUTEX_KEY_TAG
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.WORKLOAD_ID_TAG
import io.airbyte.workload.launcher.metrics.WorkloadLauncherMetricMetadata
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStage
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.pods.KubePodClient
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import reactor.core.publisher.Mono

private val logger = KotlinLogging.logger {}

/**
 * Ensures mutual exclusion (mutex) of the underlying workload resource (e.g.
 * connection). In practical terms, this kills any pods that exist for the
 * supplied key, ensuring no two workloads operate for said key simultaneously.
 */
@Singleton
@Named("mutex")
open class EnforceMutexStage(
  private val launcher: KubePodClient,
  metricPublisher: CustomMetricPublisher,
) : LaunchStage(metricPublisher) {
  @Trace(operationName = MeterFilterFactory.LAUNCH_PIPELINE_STAGE_OPERATION_NAME, resourceName = "EnforceMutexStage")
  @Instrument(
    start = "WORKLOAD_STAGE_START",
    end = "WORKLOAD_STAGE_DONE",
    duration = "WORKLOAD_STAGE_DURATION",
    tags = [Tag(key = MeterFilterFactory.STAGE_NAME_TAG, value = "mutex")],
  )
  override fun apply(input: LaunchStageIO): Mono<LaunchStageIO> {
    return super.apply(input)
  }

  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    val workloadId = input.msg.workloadId
    val key = input.msg.mutexKey

    if (key == null) {
      logger.info { "No mutex key specified for workload: $workloadId. Continuing..." }
      return input
    }

    logger.info { "Mutex key: $key specified for workload: $workloadId. Attempting to delete existing pods..." }

    val deleted = launcher.deleteMutexPods(key)
    if (deleted) {
      logger.info { "Existing pods for mutex key: $key deleted." }
      metricPublisher.count(
        WorkloadLauncherMetricMetadata.PODS_DELETED_FOR_MUTEX_KEY,
        MetricAttribute(WORKLOAD_ID_TAG, input.msg.workloadId),
        MetricAttribute(MUTEX_KEY_TAG, key),
      )
    } else {
      logger.info { "Mutex key: $key specified for workload: $workloadId found no existing pods. Continuing..." }
    }

    return input
  }

  override fun getStageName(): StageName {
    return StageName.MUTEX
  }
}
