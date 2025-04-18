/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import datadog.trace.api.Trace
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.annotations.Instrument
import io.airbyte.metrics.annotations.Tag
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.launcher.metrics.MeterFilterFactory
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStage
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.pods.KubePodClient
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import reactor.core.publisher.Mono

private val logger = KotlinLogging.logger {}

/**
 * Checks if pods with a given workload id already exist. If they do, we don't
 * need to do anything, so we no-op and skip to the end of the pipeline.
 */
@Singleton
@Named("check")
open class CheckStatusStage(
  private val podClient: KubePodClient,
  metricClient: MetricClient,
) : LaunchStage(metricClient) {
  @Trace(operationName = MeterFilterFactory.LAUNCH_PIPELINE_STAGE_OPERATION_NAME, resourceName = "CheckStatusStage")
  @Instrument(
    start = "WORKLOAD_STAGE_START",
    end = "WORKLOAD_STAGE_DONE",
    tags = [Tag(key = MetricTags.STAGE_NAME_TAG, value = "check_status")],
  )
  override fun apply(input: LaunchStageIO): Mono<LaunchStageIO> = super.apply(input)

  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    val podExists = podClient.podsExistForAutoId(input.msg.autoId)

    if (podExists) {
      logger.info {
        "Found pods running for workload ${input.msg.workloadId}. Setting status to RUNNING and SKIP flag to true."
      }
      metricClient.count(
        metric = OssMetricsRegistry.WORKLOAD_ALREADY_RUNNING,
        attributes =
          arrayOf(
            MetricAttribute(MetricTags.WORKLOAD_TYPE_TAG, input.msg.workloadType.toString()),
          ),
      )

      return input.apply {
        skip = true
      }
    }

    logger.info { "No pod found running for workload ${input.msg.workloadId}" }
    return input
  }

  override fun getStageName(): StageName = StageName.CHECK_STATUS
}
