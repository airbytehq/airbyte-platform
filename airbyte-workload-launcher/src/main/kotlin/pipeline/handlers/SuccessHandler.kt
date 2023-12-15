/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.handlers

import io.airbyte.api.client.invoker.generated.ApiException
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.workload.launcher.client.WorkloadApiClient
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.metrics.MeterFilterFactory
import io.airbyte.workload.launcher.metrics.WorkloadLauncherMetricMetadata
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.withLoggingContext
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.Optional
import java.util.function.Function

private val logger = KotlinLogging.logger {}

@Singleton
class SuccessHandler(
  private val apiClient: WorkloadApiClient,
  private val metricPublisher: CustomMetricPublisher,
  @Named("logMsgTemplate") private val logMsgTemplate: Optional<Function<String, String>>,
) {
  fun accept(io: LaunchStageIO) {
    withLoggingContext(io.logCtx) {
      metricPublisher.count(
        WorkloadLauncherMetricMetadata.WORKLOAD_PROCESSED,
        MetricAttribute(MeterFilterFactory.WORKLOAD_ID_TAG, io.msg.workloadId),
        MetricAttribute(MeterFilterFactory.STATUS_TAG, MeterFilterFactory.SUCCESS_STATUS),
      )
      if (io.msg.startTimeMs != null) {
        val timeElapsed = System.currentTimeMillis() - io.msg.startTimeMs
        metricPublisher.gauge(
          WorkloadLauncherMetricMetadata.PRODUCER_TO_POD_STARTED_LATENCY_MS,
          timeElapsed,
          { it.toDouble() },
        )
      }

      try {
        apiClient.updateStatusToLaunched(io.msg.workloadId)
      } catch (e: Exception) {
        val errorMsg = "Failed to update workload status to launched. Workload may be reprocessed on restart."
        if (e is ApiException && e.code == 410) {
          logger.debug(e) { errorMsg }
        } else {
          logger.warn(e) { errorMsg }
        }
      }

      logger.info { logMsgTemplate.orElse { id: String -> "Pipeline completed for workload: $id." }.apply(io.msg.workloadId) }
    }
  }
}
