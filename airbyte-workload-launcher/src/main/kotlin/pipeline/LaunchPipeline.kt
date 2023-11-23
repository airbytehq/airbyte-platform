package io.airbyte.workload.launcher.pipeline

import datadog.trace.api.Trace
import io.airbyte.config.helpers.LogClientSingleton
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.workload.launcher.client.StatusUpdater
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.DATA_PLANE_ID_TAG
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.LAUNCH_PIPELINE_OPERATION_NAME
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.WORKLOAD_ID_TAG
import io.airbyte.workload.launcher.metrics.WorkloadLauncherMetricMetadata
import io.airbyte.workload.launcher.pipeline.stages.BuildInputStage
import io.airbyte.workload.launcher.pipeline.stages.CheckStatusStage
import io.airbyte.workload.launcher.pipeline.stages.ClaimStage
import io.airbyte.workload.launcher.pipeline.stages.EnforceMutexStage
import io.airbyte.workload.launcher.pipeline.stages.LaunchPodStage
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.withLoggingContext
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import reactor.kotlin.core.publisher.toMono

private val logger = KotlinLogging.logger {}

@Singleton
class LaunchPipeline(
  @Value("\${airbyte.data-plane-id}") private val dataplaneId: String,
  private val claim: ClaimStage,
  private val check: CheckStatusStage,
  private val build: BuildInputStage,
  private val mutex: EnforceMutexStage,
  private val launch: LaunchPodStage,
  private val statusUpdater: StatusUpdater,
  private val metricPublisher: CustomMetricPublisher,
  private val processClaimedScheduler: Scheduler,
) {
  @Trace(operationName = LAUNCH_PIPELINE_OPERATION_NAME)
  fun accept(msg: LauncherInput) {
    metricPublisher.count(WorkloadLauncherMetricMetadata.WORKLOAD_RECEIVED, MetricAttribute(WORKLOAD_ID_TAG, msg.workloadId))
    runPipeline(msg, Schedulers.immediate())
  }

  @Trace(operationName = LAUNCH_PIPELINE_OPERATION_NAME)
  fun processClaimed(msg: LauncherInput) {
    metricPublisher.count(WorkloadLauncherMetricMetadata.WORKLOAD_CLAIM_RESUMED, MetricAttribute(WORKLOAD_ID_TAG, msg.workloadId))
    runPipeline(msg, processClaimedScheduler)
  }

  private fun runPipeline(
    msg: LauncherInput,
    scheduler: Scheduler,
  ) {
    addTagsToTrace(msg)
    withLoggingContext(LogClientSingleton.JOB_LOG_PATH_MDC_KEY to msg.logPath) {
      LaunchStageIO(msg)
        .toMono()
        .flatMap(claim)
        .flatMap(check)
        .flatMap(build)
        .flatMap(mutex)
        .flatMap(launch)
        .onErrorResume(this::handleError)
        // doOnSuccess is always called because we resume errors
        .doOnSuccess { r ->
          if (r == null) {
            metricPublisher.count(
              WorkloadLauncherMetricMetadata.WORKLOAD_PROCESSED_UNSUCCESSFULLY,
              MetricAttribute(WORKLOAD_ID_TAG, msg.workloadId),
            )
            logger.info { "Pipeline completed after error for workload: ${msg.workloadId}." }
          } else {
            metricPublisher.count(
              WorkloadLauncherMetricMetadata.WORKLOAD_PROCESSED_SUCCESSFULLY,
              MetricAttribute(WORKLOAD_ID_TAG, msg.workloadId),
            )
            logger.info { "Pipeline completed for workload: ${r.msg.workloadId}." }
          }
        }
        .subscribeOn(scheduler)
        .subscribe()
    }
  }

  private fun addTagsToTrace(msg: LauncherInput) {
    val commonTags = hashMapOf<String, Any>()
    commonTags[DATA_PLANE_ID_TAG] = dataplaneId
    commonTags[WORKLOAD_ID_TAG] = msg.workloadId
    ApmTraceUtils.addTagsToTrace(commonTags)
  }

  private fun handleError(e: Throwable): Mono<LaunchStageIO> {
    logger.error(e) { ("Pipeline Error") }
    if (e is StageError) {
      ApmTraceUtils.addExceptionToTrace(e)
      statusUpdater.reportFailure(e)
    }
    return Mono.empty()
  }
}
