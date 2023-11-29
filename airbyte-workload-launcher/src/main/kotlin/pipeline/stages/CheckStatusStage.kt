package io.airbyte.workload.launcher.pipeline.stages

import datadog.trace.api.Trace
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.workload.launcher.client.WorkloadApiClient
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.LAUNCH_PIPELINE_STAGE_OPERATION_NAME
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.WORKLOAD_ID_TAG
import io.airbyte.workload.launcher.metrics.WorkloadLauncherMetricMetadata
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStage
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.pods.KubePodClient
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

/**
 * Checks if pods with a given workload id already exist. If they are, we don't
 * need to do anything, so we no-op and skip to the end of the pipeline.
 */
@Singleton
@Named("check")
class CheckStatusStage(
  private val apiClient: WorkloadApiClient,
  private val kubeClient: KubePodClient,
  private val customMetricPublisher: CustomMetricPublisher,
) : LaunchStage {
  @Trace(operationName = LAUNCH_PIPELINE_STAGE_OPERATION_NAME)
  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    if (kubeClient.podsExistForWorkload(input.msg.workloadId)) {
      logger.info {
        "Found pods running for workload ${input.msg.workloadId}. Setting status to RUNNING and SKIP flag to true."
      }
      customMetricPublisher.count(WorkloadLauncherMetricMetadata.WORKLOAD_ALREADY_RUNNING, MetricAttribute(WORKLOAD_ID_TAG, input.msg.workloadId))

      try {
        apiClient.updateStatusToRunning(input.msg.workloadId)
      } catch (e: Exception) {
        logger.error(e) { "Failed to update workload: ${input.msg.workloadId} status to RUNNING. Ignoring." }
      }

      return input.apply {
        skip = true
      }
    }

    logger.info { "No pod found running for workload ${input.msg.workloadId}" }
    return input
  }

  override fun getStageName(): StageName {
    return StageName.CHECK_STATUS
  }
}
