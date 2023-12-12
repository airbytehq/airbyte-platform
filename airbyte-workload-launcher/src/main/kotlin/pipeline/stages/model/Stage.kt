package io.airbyte.workload.launcher.pipeline.stages.model

import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.FAILURE_STATUS
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.STAGE_NAME_TAG
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.STATUS_TAG
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.SUCCESS_STATUS
import io.airbyte.workload.launcher.metrics.WorkloadLauncherMetricMetadata
import io.airbyte.workload.launcher.pipeline.stages.StageName
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.withLoggingContext
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono
import java.util.function.Function

typealias StageFunction<T> = Function<T, Mono<T>>

private val logger = KotlinLogging.logger {}

abstract class Stage<T : StageIO>(protected val metricPublisher: CustomMetricPublisher) : StageFunction<T> {
  override fun apply(input: T): Mono<T> {
    metricPublisher.count(
      WorkloadLauncherMetricMetadata.WORKLOAD_STAGE_START,
      MetricAttribute(STAGE_NAME_TAG, getStageName().name),
    )
    withLoggingContext(input.logCtx) {
      if (skipStage(input)) {
        logger.info { "SKIP Stage: ${getStageName()} — (workloadId = ${input.msg.workloadId})" }
        return input.toMono()
      }

      logger.info { "APPLY Stage: ${getStageName()} — (workloadId = ${input.msg.workloadId})" }

      var success = true
      return try {
        applyStage(input).toMono()
      } catch (t: Throwable) {
        success = false
        ApmTraceUtils.addExceptionToTrace(t)
        Mono.error(StageError(input, getStageName(), t))
      } finally {
        metricPublisher.count(
          WorkloadLauncherMetricMetadata.WORKLOAD_STAGE_DONE,
          MetricAttribute(STAGE_NAME_TAG, getStageName().name),
          MetricAttribute(STATUS_TAG, if (success) SUCCESS_STATUS else FAILURE_STATUS),
        )
      }
    }
  }

  abstract fun applyStage(input: T): T

  abstract fun skipStage(input: StageIO): Boolean

  abstract fun getStageName(): StageName
}

abstract class LaunchStage(metricPublisher: CustomMetricPublisher) : Stage<LaunchStageIO>(metricPublisher) {
  override fun skipStage(input: StageIO): Boolean {
    return input !is LaunchStageIO || input.skip
  }
}

class StageError(
  val io: StageIO,
  val stageName: StageName,
  override val cause: Throwable,
) : Throwable(cause)
