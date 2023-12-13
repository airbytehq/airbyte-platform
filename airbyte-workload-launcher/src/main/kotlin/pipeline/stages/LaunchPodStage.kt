package io.airbyte.workload.launcher.pipeline.stages

import datadog.trace.api.Trace
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.metrics.MeterFilterFactory
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStage
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.stages.model.SyncPayload
import io.airbyte.workload.launcher.pods.KubePodClient
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import reactor.core.publisher.Mono

private val logger = KotlinLogging.logger {}

/**
 * Launches the pods for the workload, serializing and passing through input
 * values via the injected pod client.
 */
@Singleton
@Named("launch")
class LaunchPodStage(private val launcher: KubePodClient, metricPublisher: CustomMetricPublisher) : LaunchStage(metricPublisher) {
  @Trace(operationName = MeterFilterFactory.LAUNCH_PIPELINE_STAGE_OPERATION_NAME, resourceName = "LaunchPodStage")
  override fun apply(input: LaunchStageIO): Mono<LaunchStageIO> {
    return super.apply(input)
  }

  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    val payload = input.payload!!

    when (payload) {
      is SyncPayload -> launcher.launchReplication(payload.input!!, input.msg)
      else -> throw NotImplementedError("")
    }

    return input
  }

  override fun getStageName(): StageName {
    return StageName.LAUNCH
  }
}
