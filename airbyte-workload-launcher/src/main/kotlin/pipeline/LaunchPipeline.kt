/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline

import datadog.trace.api.Trace
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.launcher.client.LogContextFactory
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.LAUNCH_PIPELINE_OPERATION_NAME
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput
import io.airbyte.workload.launcher.pipeline.handlers.FailureHandler
import io.airbyte.workload.launcher.pipeline.handlers.SuccessHandler
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStage
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.kotlin.core.publisher.toMono
import kotlin.time.TimeSource
import kotlin.time.toJavaDuration

@Singleton
class LaunchPipeline(
  @Value("\${airbyte.data-plane-id}") private val dataplaneId: String,
  @Named("build") private val build: LaunchStage,
  @Named("loadShed") private val loadShed: LaunchStage,
  @Named("claim") private val claim: LaunchStage,
  @Named("check") private val check: LaunchStage,
  @Named("mutex") private val mutex: LaunchStage,
  @Named("launch") private val launch: LaunchStage,
  private val successHandler: SuccessHandler,
  private val failureHandler: FailureHandler,
  private val metricClient: MetricClient,
  private val ctxFactory: LogContextFactory,
) {
  @Trace(operationName = LAUNCH_PIPELINE_OPERATION_NAME)
  fun accept(msg: LauncherInput) {
    val startTime = TimeSource.Monotonic.markNow()
    metricClient.count(
      metric = OssMetricsRegistry.WORKLOAD_RECEIVED,
      attributes =
        arrayOf(
          MetricAttribute(MetricTags.WORKLOAD_TYPE_TAG, msg.workloadType.toString()),
        ),
    )
    val disposable =
      buildPipeline(msg)
        .subscribeOn(Schedulers.immediate())
        .subscribe()
    metricClient
      .timer(
        metric = OssMetricsRegistry.WORKLOAD_LAUNCH_DURATION,
        attributes =
          arrayOf(
            MetricAttribute(MetricTags.WORKLOAD_TYPE_TAG, msg.workloadType.toString()),
          ),
      )?.record(startTime.elapsedNow().toJavaDuration())
    disposable.dispose()
  }

  fun buildPipeline(msg: LauncherInput): Mono<LaunchStageIO> {
    addTagsToTrace(msg)
    val loggingCtx = ctxFactory.create(msg)
    val input = LaunchStageIO(msg, loggingCtx)

    return input
      .toMono()
      .flatMap(build)
      .flatMap(loadShed)
      .flatMap(claim)
      .flatMap(check)
      .flatMap(mutex)
      .flatMap(launch)
      .onErrorResume { e -> failureHandler.apply(e, input) }
      .doOnNext(successHandler::accept)
  }

  private fun addTagsToTrace(msg: LauncherInput) {
    val commonTags = hashMapOf<String, Any>()
    commonTags[MetricTags.DATA_PLANE_ID_TAG] = dataplaneId
    commonTags[MetricTags.WORKLOAD_ID_TAG] = msg.workloadId
    ApmTraceUtils.addTagsToTrace(commonTags)
  }
}
