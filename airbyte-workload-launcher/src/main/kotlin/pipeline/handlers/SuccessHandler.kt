package io.airbyte.workload.launcher.pipeline.handlers

import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.metrics.MeterFilterFactory
import io.airbyte.workload.launcher.metrics.WorkloadLauncherMetricMetadata
import io.airbyte.workload.launcher.model.withMDCLogPath
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
class SuccessHandler(
  private val metricPublisher: CustomMetricPublisher,
  private val logMsgTemplate: (String) -> String = { id: String -> "Pipeline completed for workload: $id." },
) {
  fun accept(io: LaunchStageIO) {
    withMDCLogPath(io.msg.logPath) {
      metricPublisher.count(
        WorkloadLauncherMetricMetadata.WORKLOAD_PROCESSED_SUCCESSFULLY,
        MetricAttribute(MeterFilterFactory.WORKLOAD_ID_TAG, io.msg.workloadId),
      )
      logger.info { logMsgTemplate(io.msg.workloadId) }
    }
  }
}
