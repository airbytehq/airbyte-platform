package io.airbyte.workload.launcher.pipeline.stages

import datadog.trace.api.Trace
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.workload.launcher.client.StatusUpdater
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.LAUNCH_PIPELINE_STAGE_OPERATION_NAME
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.WORKLOAD_ID_TAG
import io.airbyte.workload.launcher.metrics.WorkloadLauncherMetricMetadata
import io.airbyte.workload.launcher.pipeline.LaunchStage
import io.airbyte.workload.launcher.pipeline.LaunchStageIO
import io.airbyte.workload.launcher.pods.KubePodClient
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

private val LOGGER = KotlinLogging.logger {}

@Singleton
class CheckStatusStage(
  private val statusClient: StatusUpdater,
  private val kubeClient: KubePodClient,
  private val customMetricPublisher: CustomMetricPublisher,
) : LaunchStage {
  @Trace(operationName = LAUNCH_PIPELINE_STAGE_OPERATION_NAME)
  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    LOGGER.info { "Stage: ${javaClass.simpleName} (workloadId = ${input.msg.workloadId})" }
    return if (kubeClient.podsExistForWorkload(input.msg.workloadId)) {
      LOGGER.info {
        "Found pods running for workload ${input.msg.workloadId}, setting status as running and skip flag as true"
      }
      customMetricPublisher.count(WorkloadLauncherMetricMetadata.WORKLOAD_ALREADY_RUNNING, MetricAttribute(WORKLOAD_ID_TAG, input.msg.workloadId))
      statusClient.updateStatusToRunning(input.msg.workloadId)
      input.apply {
        skip = true
      }
    } else {
      LOGGER.info { "No pod found running for workload ${input.msg.workloadId}" }
      input
    }
  }

  override fun getStageName(): StageName {
    return StageName.CHECK_STATUS
  }
}
