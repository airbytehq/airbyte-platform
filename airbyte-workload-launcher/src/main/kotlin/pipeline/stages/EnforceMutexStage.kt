package io.airbyte.workload.launcher.pipeline.stages

import datadog.trace.api.Trace
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.LAUNCH_PIPELINE_STAGE_OPERATION_NAME
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.MUTEX_KEY_TAG
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.WORKLOAD_ID_TAG
import io.airbyte.workload.launcher.metrics.WorkloadLauncherMetricMetadata
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStage
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.pods.KubePodClient
import io.airbyte.workload.launcher.pods.PodLabeler
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
@Named("mutex")
class EnforceMutexStage(
  private val launcher: KubePodClient,
  private val metricPublisher: CustomMetricPublisher,
  private val labeler: PodLabeler,
) : LaunchStage {
  @Trace(operationName = LAUNCH_PIPELINE_STAGE_OPERATION_NAME)
  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    val replInput = input.replicationInput!!

    val deleted = launcher.deleteMutexPods(replInput)
    if (deleted) {
      val key = labeler.getMutexKey(replInput)
      logger.info { "Existing pods for key: $key deleted." }
      metricPublisher.count(
        WorkloadLauncherMetricMetadata.PODS_DELETED_FOR_MUTEX_KEY,
        MetricAttribute(WORKLOAD_ID_TAG, input.msg.workloadId),
        MetricAttribute(MUTEX_KEY_TAG, key),
      )
    }

    return input
  }

  override fun getStageName(): StageName {
    return StageName.MUTEX
  }
}
