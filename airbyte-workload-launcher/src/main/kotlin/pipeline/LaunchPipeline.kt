/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline

import datadog.trace.api.Trace
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.LAUNCH_PIPELINE_OPERATION_NAME
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput
import io.airbyte.workload.launcher.pipeline.handlers.FailureHandler
import io.airbyte.workload.launcher.pipeline.handlers.SuccessHandler
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStage
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import jakarta.inject.Named
import jakarta.inject.Singleton
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.kotlin.core.publisher.toMono

@Singleton
class LaunchPipeline(
  @Named("build") private val build: LaunchStage,
  @Named("loadShed") private val loadShed: LaunchStage,
  @Named("claim") private val claim: LaunchStage,
  @Named("check") private val check: LaunchStage,
  @Named("mutex") private val mutex: LaunchStage,
  @Named("architecture") private val architecture: LaunchStage,
  @Named("launch") private val launch: LaunchStage,
  private val successHandler: SuccessHandler,
  private val failureHandler: FailureHandler,
  private val ingressAdapter: PipelineIngressAdapter,
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
    val io = ingressAdapter.apply(input)

    return io
      .toMono()
      .flatMap(build)
      .flatMap(claim)
      .flatMap(loadShed)
      .flatMap(check)
      .flatMap(mutex)
      .flatMap(architecture)
      .flatMap(launch)
      .onErrorResume { e -> failureHandler.accept(e, io) }
      .doOnNext(successHandler::accept)
  }

  /*
   * Applies the pipeline to a stream of inputs.
   */
  fun apply(publisher: Flux<LauncherInput>): Flux<LaunchStageIO> =
    publisher
      .map(ingressAdapter::apply)
      .flatMap(build)
      .flatMap(claim)
      .flatMap(loadShed)
      .flatMap(check)
      .flatMap(mutex)
      .flatMap(architecture)
      .flatMap(launch)
      .onErrorContinue(failureHandler::accept)
      .doOnNext(successHandler::accept)
}
