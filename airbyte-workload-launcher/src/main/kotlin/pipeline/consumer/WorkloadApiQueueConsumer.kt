/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.consumer

import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.launcher.metrics.ReactorMetricsWrapper
import io.airbyte.workload.launcher.pipeline.LaunchPipeline
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import reactor.core.scheduler.Schedulers

private val logger = KotlinLogging.logger { }

/**
 * Controls the initialization of pollers and pipelines for both high and default priority workloads.
 */
@Singleton
class WorkloadApiQueueConsumer(
  private val reactorMetricsWrapper: ReactorMetricsWrapper,
  private val pipeline: LaunchPipeline,
  @Named("highPriorityQueuePoller") private val highPriorityQueuePoller: WorkloadApiQueuePoller,
  @Named("defaultPriorityQueuePoller") private val defaultPriorityQueuePoller: WorkloadApiQueuePoller,
  @Value("\${airbyte.workload-launcher.parallelism.default-queue}") private val defaultPriorityParallelism: Int,
  @Value("\${airbyte.workload-launcher.parallelism.high-priority-queue}") private val highPriorityParallelism: Int,
  @Value("\${airbyte.workload-launcher.consumer.queue-task-cap}") private val queueTaskCap: Int,
) {
  companion object {
    const val QUEUE_CONSUMER_METRIC_PREFIX = "workload_queue_consumer"
    const val DEFAULT_PRIORITY_NAME = "default"
    const val HIGH_PRIORITY_NAME = "high"
  }

  fun initialize(dataplaneGroupId: String) {
    logger.info { "Initializing ApiQueueConsumer for $dataplaneGroupId" }

    val defaultPriorityThreadPool =
      reactorMetricsWrapper.asTimedScheduler(
        scheduler = Schedulers.newBoundedElastic(defaultPriorityParallelism, queueTaskCap, DEFAULT_PRIORITY_NAME),
        metricPrefix = QUEUE_CONSUMER_METRIC_PREFIX,
        MetricAttribute(MetricTags.QUEUE_NAME_TAG, "$dataplaneGroupId-$DEFAULT_PRIORITY_NAME"),
        MetricAttribute(MetricTags.DATA_PLANE_GROUP_TAG, dataplaneGroupId),
        MetricAttribute(MetricTags.PRIORITY_TAG, DEFAULT_PRIORITY_NAME),
      )
    val highPriorityThreadPool =
      reactorMetricsWrapper.asTimedScheduler(
        scheduler = Schedulers.newBoundedElastic(highPriorityParallelism, queueTaskCap, HIGH_PRIORITY_NAME),
        metricPrefix = QUEUE_CONSUMER_METRIC_PREFIX,
        MetricAttribute(MetricTags.QUEUE_NAME_TAG, "$dataplaneGroupId-$HIGH_PRIORITY_NAME"),
        MetricAttribute(MetricTags.DATA_PLANE_GROUP_TAG, dataplaneGroupId),
        MetricAttribute(MetricTags.PRIORITY_TAG, HIGH_PRIORITY_NAME),
      )

    val defaultPriorityQueuePollerFlux =
      defaultPriorityQueuePoller
        .initialize(dataplaneGroupId)
        .flux
        .parallel(defaultPriorityParallelism)
        .runOn(defaultPriorityThreadPool)

    val highPriorityQueuePollerFlux =
      highPriorityQueuePoller
        .initialize(dataplaneGroupId)
        .flux
        .parallel(highPriorityParallelism)
        .runOn(highPriorityThreadPool)

    highPriorityQueuePollerFlux
      .flatMap(pipeline::buildPipeline)
      .subscribe()

    defaultPriorityQueuePollerFlux
      .flatMap(pipeline::buildPipeline)
      .subscribe()
  }

  fun suspendPolling() {
    highPriorityQueuePoller.suspendPolling()
    defaultPriorityQueuePoller.suspendPolling()
  }

  fun resumePolling() {
    highPriorityQueuePoller.resumePolling()
    defaultPriorityQueuePoller.resumePolling()
  }

  fun isSuspended(queueName: String): Boolean = defaultPriorityQueuePoller.isSuspended()
}
