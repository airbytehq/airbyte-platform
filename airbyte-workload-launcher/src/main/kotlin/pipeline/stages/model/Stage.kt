/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages.model

import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.FAILURE_STATUS
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.SUCCESS_STATUS
import io.airbyte.workload.launcher.pipeline.stages.StageName
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.withLoggingContext
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono
import java.util.function.Function
import kotlin.time.TimeSource
import kotlin.time.toJavaDuration

typealias StageFunction<T> = Function<T, Mono<T>>

private val logger = KotlinLogging.logger {}

abstract class Stage<T : StageIO>(
  protected val metricClient: MetricClient,
) : StageFunction<T> {
  override fun apply(input: T): Mono<T> {
    val detailedInfo = "(workloadId=${input.msg.workloadId})"

    withLoggingContext(input.logCtx) {
      if (skipStage(input)) {
        logger.info { "SKIP Stage: ${getStageName()} — $detailedInfo" }
        return input.toMono()
      }

      val startTime = TimeSource.Monotonic.markNow()
      var success = true

      logger.info { "APPLY Stage: ${getStageName()} — $detailedInfo" }

      return try {
        applyStage(input).toMono()
      } catch (t: Throwable) {
        success = false
        ApmTraceUtils.addExceptionToTrace(t)
        Mono.error(StageError(input, getStageName(), t))
      } finally {
        metricClient
          .timer(
            metric = OssMetricsRegistry.WORKLOAD_STAGE_DURATION,
            attributes =
              getMetricAttrs(input).toTypedArray() +
                arrayOf(
                  MetricAttribute(MetricTags.STAGE_NAME_TAG, getStageName().toString()),
                  MetricAttribute(MetricTags.STATUS, if (success) SUCCESS_STATUS else FAILURE_STATUS),
                ),
          )?.record(startTime.elapsedNow().toJavaDuration())
      }
    }
  }

  abstract fun applyStage(input: T): T

  abstract fun skipStage(input: StageIO): Boolean

  abstract fun getStageName(): StageName

  abstract fun getMetricAttrs(input: T): List<MetricAttribute>
}

abstract class LaunchStage(
  metricClient: MetricClient,
) : Stage<LaunchStageIO>(metricClient) {
  override fun skipStage(input: StageIO): Boolean = input !is LaunchStageIO || input.skip

  override fun getMetricAttrs(input: LaunchStageIO): List<MetricAttribute> =
    listOf(MetricAttribute(MetricTags.WORKLOAD_TYPE_TAG, input.msg.workloadType.toString()))
}

class StageError(
  val io: StageIO,
  val stageName: StageName,
  override val cause: Throwable,
) : Throwable(cause)
