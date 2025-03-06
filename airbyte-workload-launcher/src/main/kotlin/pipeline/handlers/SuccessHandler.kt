/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.handlers

import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.launcher.client.WorkloadApiClient
import io.airbyte.workload.launcher.metrics.MeterFilterFactory
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.withLoggingContext
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import java.util.Optional
import java.util.function.Function

private val logger = KotlinLogging.logger {}

@Singleton
class SuccessHandler(
  private val apiClient: WorkloadApiClient,
  private val metricClient: MetricClient,
  @Named("logMsgTemplate") private val logMsgTemplate: Optional<Function<String, String>>,
) {
  fun accept(io: LaunchStageIO) {
    withLoggingContext(io.logCtx) {
      metricClient.count(
        metric = OssMetricsRegistry.WORKLOAD_PROCESSED,
        attributes =
          arrayOf(
            MetricAttribute(MetricTags.WORKLOAD_TYPE_TAG, io.msg.workloadType.toString()),
            MetricAttribute(MetricTags.STATUS_TAG, MeterFilterFactory.SUCCESS_STATUS),
          ),
      )
      if (io.msg.startTimeMs != null) {
        metricClient.gauge(
          metric = OssMetricsRegistry.PRODUCER_TO_POD_STARTED_LATENCY_MS,
          stateObject = System.currentTimeMillis() - io.msg.startTimeMs,
          function = { it.toDouble() },
          attributes =
            arrayOf(
              MetricAttribute(MetricTags.WORKLOAD_TYPE_TAG, io.msg.workloadType.toString()),
            ),
        )
      }

      // If we skipped then we didn't launch the workload on this run, so we don't set its status to "launched".
      if (!io.skip) {
        try {
          apiClient.updateStatusToLaunched(io.msg.workloadId)
        } catch (e: Exception) {
          val errorMsg = "Failed to update workload status to launched. Workload may be reprocessed on restart."
          if (e is ClientException && e.statusCode == 410) {
            logger.debug(e) {
              errorMsg +
                "Exception: $e\n" +
                "message: ${e.message}\n" +
                "stackTrace: ${e.stackTrace}\n"
            }
          } else {
            logger.warn(e) {
              errorMsg +
                "Exception: $e\n" +
                "message: ${e.message}\n" +
                "stackTrace: ${e.stackTrace}\n"
            }
          }
        }
      }

      logger.info { logMsgTemplate.orElse { id: String -> "Pipeline completed for workload: $id." }.apply(io.msg.workloadId) }
    }
  }
}
