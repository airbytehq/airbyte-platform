/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.consumer

import io.airbyte.featureflag.DataplaneGroup
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.WorkloadPollerUsesJitter
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.api.client.model.generated.Workload
import io.airbyte.workload.api.client.model.generated.WorkloadPriority
import io.airbyte.workload.launcher.client.WorkloadApiClient
import io.airbyte.workload.launcher.model.toLauncherInput
import io.github.oshai.kotlinlogging.KotlinLogging
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration
import kotlin.concurrent.Volatile
import kotlin.random.Random
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.toJavaDuration

private val logger = KotlinLogging.logger {}

/**
 * Polls the workload api backed queue with given configuration.
 * Emits a Flux<LauncherInput> for consumption by the pipeline.
 *
 * Starts in a suspended state until #resumePolling is called.
 */
class WorkloadApiQueuePoller(
  private val workloadApiClient: WorkloadApiClient,
  private val metricClient: MetricClient,
  private val featureFlagClient: FeatureFlagClient,
  private val pollSizeItems: Int,
  private val pollIntervalSeconds: Long,
  private val priority: WorkloadPriority,
) {
  @Volatile
  private var suspended = true
  private var initialized = false
  private lateinit var groupId: String
  lateinit var flux: Flux<LauncherInput>
    private set

  fun initialize(groupId: String): WorkloadApiQueuePoller {
    if (initialized) return this

    logger.info { "Initalizing ApiQueuePoller with $groupId and $priority" }

    this.groupId = groupId
    this.flux = buildInputFlux()
    initialized = true

    return this
  }

  fun suspendPolling() {
    logger.info { "Suspending ApiQueuePoller with $groupId and $priority" }
    suspended = true
  }

  fun resumePolling() {
    logger.info { "Resuming ApiQueuePoller with $groupId and $priority" }
    suspended = false
  }

  fun isSuspended(): Boolean = suspended

  private fun buildInputFlux(): Flux<LauncherInput> {
    val interval =
      if (useJitter()) {
        Flux
          .interval(Duration.ofSeconds(pollIntervalSeconds))
          .onBackpressureDrop()
          .delayUntil { Mono.delay(Random.nextInt(0, 100).milliseconds.toJavaDuration()) }
      } else {
        Flux.interval(Duration.ofSeconds(pollIntervalSeconds)).onBackpressureDrop()
      }

    val pollFlux: Flux<Workload> =
      Flux.create { sink ->
        val results = workloadApiClient.pollQueue(groupId, priority, pollSizeItems)
        metricClient.count(
          OssMetricsRegistry.WORKLOAD_QUEUE_MESSAGES_POLLED,
          results.size.toLong(),
          MetricAttribute(MetricTags.DATA_PLANE_GROUP_TAG, groupId),
          MetricAttribute(MetricTags.PRIORITY_TAG, priority.toString()),
        )
        results.forEach(sink::next)
        sink.complete()
      }

    return interval
      .filter { !isSuspended() }
      .flatMap { pollFlux }
      .map(Workload::toLauncherInput)
      .onErrorContinue(this::handlePollError)
  }

  private fun useJitter(): Boolean = featureFlagClient.boolVariation(WorkloadPollerUsesJitter, DataplaneGroup(groupId))

  private fun handlePollError(
    e: Throwable,
    _input: Any?,
  ): Mono<LauncherInput> {
    metricClient.count(OssMetricsRegistry.WORKLOAD_QUEUE_CONSUMER_FAILURE, 1)
    logger.warn { "Error encountered in poller: $e\n Ignoring..." }
    return Mono.empty()
  }
}
