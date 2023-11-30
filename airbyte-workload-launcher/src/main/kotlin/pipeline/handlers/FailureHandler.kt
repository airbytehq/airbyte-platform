package io.airbyte.workload.launcher.pipeline.handlers

import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.workload.launcher.client.WorkloadApiClient
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.metrics.MeterFilterFactory
import io.airbyte.workload.launcher.metrics.WorkloadLauncherMetricMetadata
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.stages.model.StageError
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.withLoggingContext
import jakarta.inject.Singleton
import reactor.core.publisher.Mono

private val logger = KotlinLogging.logger {}

@Singleton
class FailureHandler(
  private val apiClient: WorkloadApiClient,
  private val metricPublisher: CustomMetricPublisher,
  private val logMsgTemplate: (String) -> String = { id -> "Pipeline aborted after error for workload: $id." },
) {
  fun apply(
    e: Throwable,
    io: LaunchStageIO,
  ): Mono<LaunchStageIO> {
    withLoggingContext(io.logCtx) {
      logger.error(e) { ("Pipeline Error") }
      ApmTraceUtils.addExceptionToTrace(e)

      if (e is StageError) {
        apiClient.reportFailure(e)
      }

      metricPublisher.count(
        WorkloadLauncherMetricMetadata.WORKLOAD_PROCESSED_UNSUCCESSFULLY,
        MetricAttribute(MeterFilterFactory.WORKLOAD_ID_TAG, io.msg.workloadId),
      )
      logger.info { logMsgTemplate(io.msg.workloadId) }
    }

    return Mono.empty()
  }
}
