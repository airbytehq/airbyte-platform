package io.airbyte.workload.launcher.pipeline.stages.model

import datadog.trace.api.Trace
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.LAUNCH_PIPELINE_STAGE_OPERATION_NAME
import io.airbyte.workload.launcher.pipeline.stages.StageName
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.withLoggingContext
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono
import java.util.function.Function

typealias StageFunction<T> = Function<T, Mono<T>>

private val logger = KotlinLogging.logger {}

interface Stage<T : StageIO> : StageFunction<T> {
  @Trace(operationName = LAUNCH_PIPELINE_STAGE_OPERATION_NAME)
  override fun apply(input: T): Mono<T> {
    withLoggingContext(input.logCtx) {
      if (skipStage(input)) {
        logger.info { "SKIP Stage: ${getStageName()} — (workloadId = ${input.msg.workloadId})" }
        return input.toMono()
      }

      logger.info { "APPLY Stage: ${getStageName()} — (workloadId = ${input.msg.workloadId})" }

      return try {
        applyStage(input).toMono()
      } catch (t: Throwable) {
        ApmTraceUtils.addExceptionToTrace(t)
        Mono.error(StageError(input, getStageName(), t))
      }
    }
  }

  fun applyStage(input: T): T

  fun skipStage(input: StageIO): Boolean

  fun getStageName(): StageName
}

interface LaunchStage : Stage<LaunchStageIO> {
  override fun skipStage(input: StageIO): Boolean {
    return input !is LaunchStageIO || input.skip
  }
}

class StageError(
  val io: StageIO,
  val stageName: StageName,
  override val cause: Throwable,
) : Throwable(cause)
