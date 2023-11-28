package io.airbyte.workload.launcher.pipeline.handlers

import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.workload.launcher.client.StatusUpdater
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.metrics.MeterFilterFactory
import io.airbyte.workload.launcher.metrics.WorkloadLauncherMetricMetadata
import io.airbyte.workload.launcher.model.withMDCLogPath
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.stages.model.StageError
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import reactor.core.publisher.Mono

private val logger = KotlinLogging.logger {}

@Singleton
class FailureHandler(
  private val statusUpdater: StatusUpdater,
  private val metricPublisher: CustomMetricPublisher,
  private val logMsgTemplate: (String) -> String = { id -> "Pipeline aborted after error for workload: $id." },
) {
  fun apply(
    e: Throwable,
    io: LauncherInput,
  ): Mono<LaunchStageIO> {
    withMDCLogPath(io.logPath) {
      logger.error(e) { ("Pipeline Error") }
      ApmTraceUtils.addExceptionToTrace(e)

      if (e is StageError) {
        statusUpdater.reportFailure(e)
      }

      metricPublisher.count(
        WorkloadLauncherMetricMetadata.WORKLOAD_PROCESSED_UNSUCCESSFULLY,
        MetricAttribute(MeterFilterFactory.WORKLOAD_ID_TAG, io.workloadId),
      )
      logger.info { logMsgTemplate(io.workloadId) }
    }

    return Mono.empty()
  }
}
