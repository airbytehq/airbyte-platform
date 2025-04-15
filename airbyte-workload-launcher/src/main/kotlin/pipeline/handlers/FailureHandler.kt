/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.handlers

import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workers.exception.KubeClientException
import io.airbyte.workload.launcher.client.WorkloadApiClient
import io.airbyte.workload.launcher.metrics.MeterFilterFactory
import io.airbyte.workload.launcher.pipeline.stages.StageName
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.stages.model.StageError
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.withLoggingContext
import jakarta.inject.Named
import jakarta.inject.Singleton
import reactor.core.publisher.Mono
import java.util.Optional
import java.util.function.Function
import kotlin.time.toJavaDuration

private val logger = KotlinLogging.logger {}

@Singleton
class FailureHandler(
  private val apiClient: WorkloadApiClient,
  private val metricClient: MetricClient,
  @Named("logMsgTemplate") private val logMsgTemplate: Optional<Function<String, String>>,
) {
  fun accept(
    e: Throwable,
    input: Any?,
  ): Mono<LaunchStageIO> {
    // Attaching an exception here should tie it to the root span to ensure we mark it as failed.
    ApmTraceUtils.addExceptionToTrace(e)
    when (input) {
      is LaunchStageIO -> {
        withLoggingContext(input.logCtx) {
          handleStageError(e, input)
        }
      }
      else -> {
        logger.error(e) {
          "Non-Stage Pipeline Exception: $e\n" +
            "message: ${e.message}\n" +
            "stackTrace: ${e.stackTrace}\n"
        }
        metricClient.count(
          metric = OssMetricsRegistry.WORKLOAD_LAUNCHER_NON_STAGE_FAILURE,
        )
      }
    }

    return Mono.empty()
  }

  private fun handleStageError(
    e: Throwable,
    io: LaunchStageIO,
  ) {
    logger.error(e) {
      "Stage Pipeline Exception: $e\n" +
        "message: ${e.message}\n" +
        "stackTrace: ${e.stackTrace}\n"
    }

    // Ignore claim errors as another dataplane may pick up the workload
    if (e is StageError && e.stageName == StageName.CLAIM) {
      logger.warn { "Unexpected StageError for stage: ${StageName.CLAIM}. Ignoring." }
    } else {
      apiClient.reportFailure(io.msg.workloadId, e)
    }
    val attrs =
      arrayOf(
        MetricAttribute(MetricTags.WORKLOAD_TYPE_TAG, io.msg.workloadType.toString()),
        MetricAttribute(MetricTags.STATUS_TAG, MeterFilterFactory.FAILURE_STATUS),
      )

    if (io.receivedAt != null) {
      metricClient
        .timer(
          metric = OssMetricsRegistry.WORKLOAD_LAUNCH_DURATION,
          attributes =
            arrayOf(
              MetricAttribute(MetricTags.WORKLOAD_TYPE_TAG, io.msg.workloadType.toString()),
              MetricAttribute(MetricTags.STATUS_TAG, MeterFilterFactory.FAILURE_STATUS),
            ),
        )?.record(io.receivedAt!!.elapsedNow().toJavaDuration())
    }

    val kubeAttrs =
      buildList {
        if (e.cause is KubeClientException) {
          val clientEx = (e.cause as KubeClientException)
          add(MetricAttribute(MetricTags.KUBE_COMMAND_TYPE_TAG, clientEx.commandType.toString()))
          if (clientEx.podType != null) {
            add(MetricAttribute(MetricTags.KUBE_POD_TYPE_TAG, clientEx.podType.toString()))
          }
        }
      }

    metricClient.count(
      metric = OssMetricsRegistry.WORKLOAD_PROCESSED,
      attributes = attrs + kubeAttrs.toTypedArray(),
    )
    logger.info { logMsgTemplate.orElse { id -> "Pipeline aborted after error for workload: $id." }.apply(io.msg.workloadId) }
  }
}
