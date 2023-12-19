package io.airbyte.workload.launcher.pipeline

import datadog.trace.api.Trace
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.workload.launcher.client.LogContextFactory
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.DATA_PLANE_ID_TAG
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.LAUNCH_PIPELINE_OPERATION_NAME
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.WORKLOAD_ID_TAG
import io.airbyte.workload.launcher.metrics.WorkloadLauncherMetricMetadata
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput
import io.airbyte.workload.launcher.pipeline.handlers.FailureHandler
import io.airbyte.workload.launcher.pipeline.handlers.SuccessHandler
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStage
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.kotlin.core.publisher.toMono
import kotlin.time.TimeSource
import kotlin.time.toJavaDuration

private val logger = KotlinLogging.logger {}

@Singleton
class LaunchPipeline(
  @Value("\${airbyte.data-plane-id}") private val dataplaneId: String,
  @Named("claim") private val claim: LaunchStage,
  @Named("check") private val check: LaunchStage,
  @Named("build") private val build: LaunchStage,
  @Named("mutex") private val mutex: LaunchStage,
  @Named("launch") private val launch: LaunchStage,
  private val successHandler: SuccessHandler,
  private val failureHandler: FailureHandler,
  private val metricPublisher: CustomMetricPublisher,
  private val ctxFactory: LogContextFactory,
) {
  @Trace(operationName = LAUNCH_PIPELINE_OPERATION_NAME)
  fun accept(msg: LauncherInput) {
    val startTime = TimeSource.Monotonic.markNow()
    metricPublisher.count(WorkloadLauncherMetricMetadata.WORKLOAD_RECEIVED, MetricAttribute(WORKLOAD_ID_TAG, msg.workloadId))
    buildPipeline(msg)
      .subscribeOn(Schedulers.immediate())
      .subscribe()
    metricPublisher.timer(WorkloadLauncherMetricMetadata.WORKLOAD_LAUNCH_DURATION, startTime.elapsedNow().toJavaDuration())
  }

  fun buildPipeline(msg: LauncherInput): Mono<LaunchStageIO> {
    addTagsToTrace(msg)
    val loggingCtx = ctxFactory.create(msg)
    val input = LaunchStageIO(msg, loggingCtx)

    return input
      .toMono()
      .flatMap(claim)
      .flatMap(check)
      .flatMap(build)
      .flatMap(mutex)
      .flatMap(launch)
      .onErrorResume { e -> failureHandler.apply(e, input) }
      .doOnNext(successHandler::accept)
  }

  private fun addTagsToTrace(msg: LauncherInput) {
    val commonTags = hashMapOf<String, Any>()
    commonTags[DATA_PLANE_ID_TAG] = dataplaneId
    commonTags[WORKLOAD_ID_TAG] = msg.workloadId
    ApmTraceUtils.addTagsToTrace(commonTags)
  }
}
