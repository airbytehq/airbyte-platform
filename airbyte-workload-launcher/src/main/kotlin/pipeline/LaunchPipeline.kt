package io.airbyte.workload.launcher.pipeline

import datadog.trace.api.Trace
import io.airbyte.commons.logging.MdcScope
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
import io.airbyte.workload.launcher.pipeline.stages.LaunchPodStage
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono

private val logger = KotlinLogging.logger {}

@Singleton
class LaunchPipeline(
  @Value("\${airbyte.data-plane-id}") private val dataplaneId: String,
  private val claim: ClaimStage,
  private val check: CheckStatusStage,
  private val build: BuildInputStage,
  private val launch: LaunchPodStage,
  private val statusUpdater: StatusUpdater,
  private val metricPublisher: CustomMetricPublisher,
) {
  // todo: This is for when we get to back pressure: if we want backpressure on the Mono,
  //  we will need to create a scheduler that is used by all Monos to ensure that we have
  //  a max concurrent capacity. See the Schedulers class for details. We can even define
  //  an executor service via Micronaut configuration and inject that to pass to the
  //  scheduler (see the fromExecutorService method on Schedulers).
  @Trace(operationName = LAUNCH_PIPELINE_OPERATION_NAME)
  fun accept(msg: LauncherInput) {
    addTagsToTrace()
    setLoggingScopeForWorkload(msg).use {
      metricPublisher.count(WorkloadLauncherMetricMetadata.WORKLOAD_RECEIVED, MetricAttribute(WORKLOAD_ID_TAG, msg.workloadId))
      LaunchStageIO(msg)
        .toMono()
        .flatMap(claim)
        .flatMap(check)
        .flatMap(build)
        .flatMap(launch)
        .onErrorResume(this::handleError)
        // doOnSuccess is always called
        .doOnSuccess { r ->
          if (r == null) {
            metricPublisher.count(WorkloadLauncherMetricMetadata.WORKLOAD_PROCESSED_UNSUCCESSFULLY, MetricAttribute(WORKLOAD_ID_TAG, msg.workloadId))
          } else {
            metricPublisher.count(WorkloadLauncherMetricMetadata.WORKLOAD_PROCESSED_SUCCESSFULLY, MetricAttribute(WORKLOAD_ID_TAG, msg.workloadId))
          }
          logger.info {
            ("Success: $r")
          }
        }
        .subscribe()
    }
  }

  private fun addTagsToTrace() {
    val commonTags = hashMapOf<String, Any>()
    commonTags.put(DATA_PLANE_ID_TAG, dataplaneId)
    ApmTraceUtils.addTagsToTrace(commonTags)
  }

  private fun setLoggingScopeForWorkload(msg: LauncherInput): MdcScope {
    return MdcScope.Builder()
      .setExtraMdcEntries(mapOf(LogClientSingleton.JOB_LOG_PATH_MDC_KEY to msg.jobLogPath))
      .build()
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
