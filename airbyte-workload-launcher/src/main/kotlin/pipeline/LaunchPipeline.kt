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
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.kotlin.core.publisher.toMono
import kotlin.time.TimeSource

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
  fun accept(input: LauncherInput) {
    val disposable =
      buildPipeline(input)
        .subscribeOn(Schedulers.immediate())
        .subscribe()

    disposable.dispose()
  }

  /*
   * Builds an executable pipeline instance from a single input.
   */
  fun buildPipeline(input: LauncherInput): Mono<LaunchStageIO> {
    ingestMetrics(input)
    val io = inputToStageIO(input)

    return io
      .toMono()
      .flatMap(build)
      .flatMap(claim)
      .flatMap(loadShed)
      .flatMap(check)
      .flatMap(mutex)
      .flatMap(launch)
      .onErrorResume { e -> failureHandler.apply(e, io) }
      .doOnNext(successHandler::accept)
  }

  /*
   * Applies the pipeline to a stream of inputs.
   */
  fun apply(publisher: Flux<LauncherInput>): Flux<LaunchStageIO> =
    publisher
      .map(this::ingestMetrics)
      .map(this::inputToStageIO)
      .flatMap(build)
      .flatMap(claim)
      .flatMap(loadShed)
      .flatMap(check)
      .flatMap(mutex)
      .flatMap(launch)
//      .onErrorResume { e -> failureHandler.apply(e, input) } // TODO: fix error handling
      .doOnNext(successHandler::accept)

  private fun inputToStageIO(input: LauncherInput): LaunchStageIO {
    val loggingCtx = ctxFactory.create(input)
    return LaunchStageIO(input, loggingCtx, receivedAt = TimeSource.Monotonic.markNow())
  }

  private fun ingestMetrics(input: LauncherInput): LauncherInput {
    metricClient.count(
      metric = OssMetricsRegistry.WORKLOAD_RECEIVED,
      attributes =
        arrayOf(
          MetricAttribute(MetricTags.WORKLOAD_TYPE_TAG, input.workloadType.toString()),
        ),
    )

    val commonTags = hashMapOf<String, Any>()
    commonTags[MetricTags.DATA_PLANE_ID_TAG] = dataplaneId
    commonTags[MetricTags.WORKLOAD_ID_TAG] = input.workloadId
    ApmTraceUtils.addTagsToTrace(commonTags)

    return input
  }
}
