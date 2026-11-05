/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.annotations.Instrument
import io.airbyte.metrics.annotations.Tag
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.launcher.metrics.MeterFilterFactory
import io.airbyte.workload.launcher.pipeline.dispatch.DirectCheckEligibility
import io.airbyte.workload.launcher.pipeline.dispatch.DirectCheckRouter
import io.airbyte.workload.launcher.pipeline.dispatch.DirectJobRunnerClient
import io.airbyte.workload.launcher.pipeline.stages.model.CheckPayload
import io.airbyte.workload.launcher.pipeline.stages.model.ConnectPayload
import io.airbyte.workload.launcher.pipeline.stages.model.DiscoverCatalogPayload
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStage
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.stages.model.SpecPayload
import io.airbyte.workload.launcher.pipeline.stages.model.SyncPayload
import io.airbyte.workload.launcher.pods.KubePodClient
import io.github.oshai.kotlinlogging.KotlinLogging
import io.opentelemetry.api.trace.Span
import io.opentelemetry.instrumentation.annotations.WithSpan
import jakarta.inject.Named
import jakarta.inject.Singleton
import reactor.core.publisher.Mono

private val logger = KotlinLogging.logger {}

private const val ROUTE_RUNNER = "runner"
private const val ROUTE_POD = "pod"
private const val RUNNER_REJECTED = "runner_rejected"

/**
 * Sends the workload to where it runs. Pod workloads are launched through the injected pod client; direct jobs are
 * handed to the always-on direct job runner and get no pod. An eligible check ([DirectCheckRouter.eligibility]) goes to
 * the runner, and to a pod when the runner doesn't take it or it isn't eligible.
 */
@Singleton
@Named("dispatch")
open class DispatchStage(
  private val launcher: KubePodClient,
  metricClient: MetricClient,
  private val fusionLaunchResolver: FusionLaunchResolver,
  private val directJobRunnerClient: DirectJobRunnerClient,
  private val directCheckRouter: DirectCheckRouter,
) : LaunchStage(metricClient) {
  @WithSpan(MeterFilterFactory.LAUNCH_PIPELINE_STAGE_OPERATION_NAME)
  @Instrument(
    start = "WORKLOAD_STAGE_START",
    end = "WORKLOAD_STAGE_DONE",
    tags = [Tag(key = MetricTags.STAGE_NAME_TAG, value = "dispatch")],
  )
  override fun apply(input: LaunchStageIO): Mono<LaunchStageIO> {
    Span.current().setAttribute("resource.name", "DispatchStage")
    return super.apply(input)
  }

  // Every payload type has an arm here, so adding one forces a choice between a pod and the direct job runner.
  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    when (val payload = input.payload!!) {
      is SyncPayload -> {
        fusionLaunchResolver.resolve(payload, input.workloadId)
        if (payload.input.isReset) {
          launcher.launchReset(payload, input.msg)
        } else {
          launcher.launchReplication(payload, input.msg)
        }
      }
      is CheckPayload -> {
        val eligibility = directCheckRouter.eligibility(payload.input)
        if (eligibility == DirectCheckEligibility.ELIGIBLE && directCheckRouter.dispatch(input.msg)) {
          logger.info { "Dispatched check ${input.workloadId} to the direct job runner" }
          recordCheckRoute(ROUTE_RUNNER, eligibility.name)
        } else {
          val runnerRejected = eligibility == DirectCheckEligibility.ELIGIBLE
          val reason = if (runnerRejected) "the direct job runner did not take it" else eligibility.reason
          logger.info { "Launching check ${input.workloadId} in a pod: $reason" }
          recordCheckRoute(ROUTE_POD, if (runnerRejected) RUNNER_REJECTED else eligibility.name)
          launcher.launchCheck(payload.input, input.msg)
        }
      }
      is DiscoverCatalogPayload -> launcher.launchDiscover(payload.input, input.msg)
      is SpecPayload -> launcher.launchSpec(payload.input, input.msg)
      is ConnectPayload -> directJobRunnerClient.submit(input.msg)
    }

    return input
  }

  // Where checks go and why, so a rollout shows how many checks the runner takes and what keeps the rest on pods.
  private fun recordCheckRoute(
    destination: String,
    reason: String,
  ) = metricClient.count(
    OssMetricsRegistry.DIRECT_CHECK_ROUTE,
    1,
    MetricAttribute(MetricTags.ROUTE_DESTINATION_TAG, destination),
    MetricAttribute(MetricTags.ROUTE_REASON_TAG, reason.lowercase()),
  )

  override fun getStageName(): StageName = StageName.DISPATCH
}
