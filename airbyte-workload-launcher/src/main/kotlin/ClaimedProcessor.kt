/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher

import com.google.common.annotations.VisibleForTesting
import datadog.trace.api.Trace
import dev.failsafe.Failsafe
import dev.failsafe.RetryPolicy
import dev.failsafe.function.CheckedSupplier
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.client.generated.infrastructure.ServerException
import io.airbyte.workload.api.client.model.generated.WorkloadListRequest
import io.airbyte.workload.api.client.model.generated.WorkloadListResponse
import io.airbyte.workload.api.client.model.generated.WorkloadStatus
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.RESUME_CLAIMED_OPERATION_NAME
import io.airbyte.workload.launcher.model.toLauncherInput
import io.airbyte.workload.launcher.pipeline.LaunchPipeline
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.kotlin.core.publisher.toFlux
import java.net.SocketException
import java.net.SocketTimeoutException
import java.time.Duration

private val logger = KotlinLogging.logger {}

@Singleton
class ClaimedProcessor(
  private val apiClient: WorkloadApiClient,
  private val pipe: LaunchPipeline,
  private val metricClient: MetricClient,
  private val claimProcessorTracker: ClaimProcessorTracker,
  @Value("\${airbyte.workload-launcher.parallelism.default-queue}") parallelism: Int,
  @Named("claimedProcessorBackoffDuration") private val backoffDuration: Duration,
  @Named("claimedProcessorBackoffMaxDelay") private val backoffMaxDelay: Duration,
) {
  private val scheduler = Schedulers.newParallel("process-claimed-scheduler", parallelism)

  @Trace(operationName = RESUME_CLAIMED_OPERATION_NAME)
  fun retrieveAndProcess(dataplaneId: String) {
    addTagsToTrace(dataplaneId)
    val workloadListRequest =
      WorkloadListRequest(
        listOf(dataplaneId),
        listOf(WorkloadStatus.CLAIMED),
      )

    val workloadList = getWorkloadList(workloadListRequest)
    logger.info { "Re-hydrating ${workloadList.workloads.size} workload claim(s)..." }
    claimProcessorTracker.trackNumberOfClaimsToResume(workloadList.workloads.size)

    val msgs = workloadList.workloads.map { it.toLauncherInput() }

    processMessages(msgs)
  }

  @VisibleForTesting
  fun processMessages(msgs: List<LauncherInput>) {
    msgs
      .map { runOnClaimedScheduler(it) }
      .toFlux()
      .flatMap { w -> w }
      .collectList()
      .block()
  }

  private fun runOnClaimedScheduler(msg: LauncherInput): Mono<LaunchStageIO> {
    metricClient.count(
      metric = OssMetricsRegistry.WORKLOAD_CLAIM_RESUMED,
      attributes =
        arrayOf(
          MetricAttribute(MetricTags.WORKLOAD_TYPE_TAG, msg.workloadType.toString()),
        ),
    )
    return pipe
      .buildPipeline(msg)
      .doOnTerminate(claimProcessorTracker::trackResumed)
      .subscribeOn(scheduler)
  }

  private fun addTagsToTrace(dataplaneId: String) {
    val commonTags = hashMapOf<String, Any>()
    commonTags[MetricTags.DATA_PLANE_ID_TAG] = dataplaneId
    ApmTraceUtils.addTagsToTrace(commonTags)
  }

  private fun getWorkloadList(req: WorkloadListRequest): WorkloadListResponse =
    Failsafe
      .with(
        RetryPolicy
          .builder<Any>()
          .withBackoff(backoffDuration, backoffMaxDelay)
          .onRetry { _ -> logger.error { "Retrying to fetch workloads for dataplane(s): ${req.dataplane}" } }
          .withMaxAttempts(-1)
          .abortOn { exception ->
            when (exception) {
              // This makes us to retry only on 5XX errors
              is ServerException -> exception.statusCode / 100 != 5

              // We want to retry on most network errors
              is SocketException -> false
              is SocketTimeoutException -> false
              else -> true
            }
          }.build(),
      ).get(
        CheckedSupplier { apiClient.workloadApi.workloadList(req) },
      )
}
