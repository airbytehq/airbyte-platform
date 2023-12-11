/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

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
import jakarta.inject.Named
import jakarta.inject.Singleton
import reactor.core.publisher.Mono
import java.util.Optional
import java.util.function.Function

private val logger = KotlinLogging.logger {}

@Singleton
class FailureHandler(
  private val apiClient: WorkloadApiClient,
  private val metricPublisher: CustomMetricPublisher,
  @Named("logMsgTemplate") private val logMsgTemplate: Optional<Function<String, String>>,
) {
  fun apply(
    e: Throwable,
    io: LaunchStageIO,
  ): Mono<LaunchStageIO> {
    withLoggingContext(io.logCtx) {
      // Attaching an exception here should tie it to the root span to ensure we mark it as failed.
      ApmTraceUtils.addExceptionToTrace(e)
      logger.error(e) { ("Pipeline Error") }

      if (e is StageError) {
        apiClient.reportFailure(e)
      }

      metricPublisher.count(
        WorkloadLauncherMetricMetadata.WORKLOAD_PROCESSED,
        MetricAttribute(MeterFilterFactory.WORKLOAD_ID_TAG, io.msg.workloadId),
        MetricAttribute(MeterFilterFactory.STATUS_TAG, MeterFilterFactory.FAILURE_STATUS),
      )
      logger.info { logMsgTemplate.orElse { id -> "Pipeline aborted after error for workload: $id." }.apply(io.msg.workloadId) }
    }

    return Mono.empty()
  }
}
