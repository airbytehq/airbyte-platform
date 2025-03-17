/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.temporal

import io.airbyte.commons.temporal.queue.QueueActivity
import io.airbyte.config.messages.LauncherInputMessage
import io.airbyte.micronaut.temporal.TemporalProxyHelper
import io.airbyte.workload.launcher.pipeline.consumer.LauncherWorkflowImpl
import io.micronaut.context.annotation.Property
import io.temporal.client.WorkflowClient
import io.temporal.opentracing.OpenTracingWorkerInterceptor
import io.temporal.worker.Worker
import io.temporal.worker.WorkerFactory
import io.temporal.worker.WorkerFactoryOptions
import io.temporal.worker.WorkerOptions
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
class TemporalLauncherWorker(
  @Property(name = "airbyte.workload-launcher.parallelism.default-queue") private val defaultParalellism: Int,
  @Property(name = "airbyte.workload-launcher.temporal.default-queue.workflow-parallelism") private val defaultWorkflowParalellism: Int,
  @Property(name = "airbyte.workload-launcher.parallelism.high-priority-queue") private val highPrioParalellism: Int,
  @Property(name = "airbyte.workload-launcher.temporal.high-priority-queue.workflow-parallelism") private val highPrioWorkflowParalellism: Int,
  @Named("starterActivities") private val workloadStarterActivities: QueueActivity<LauncherInputMessage>,
  private val temporalProxyHelper: TemporalProxyHelper,
  workflowClient: WorkflowClient,
) {
  private val workerFactory = createWorkerFactory(workflowClient)
  private var defaultWorker: Worker? = null
  private var highPrioWorker: Worker? = null

  fun initialize(
    defaultQueueName: String,
    highPrioQueueName: String,
  ) {
    // stop workers if they were previously running
    suspendPolling()
    defaultWorker = getOrCreateWorker(defaultParalellism, defaultWorkflowParalellism, defaultQueueName)
    highPrioWorker = getOrCreateWorker(highPrioParalellism, highPrioWorkflowParalellism, highPrioQueueName)
    workerFactory.start()
  }

  fun resumePolling() {
    defaultWorker?.resumePolling()
    highPrioWorker?.resumePolling()
  }

  fun suspendPolling() {
    defaultWorker?.suspendPolling()
    highPrioWorker?.suspendPolling()
  }

  fun isSuspended(queueName: String): Boolean = workerFactory.tryGetWorker(queueName)?.isSuspended ?: true

  private fun createWorkerFactory(workflowClient: WorkflowClient): WorkerFactory {
    val workerFactoryOptions =
      WorkerFactoryOptions
        .newBuilder()
        .setWorkerInterceptors(OpenTracingWorkerInterceptor())
        .build()
    return WorkerFactory.newInstance(workflowClient, workerFactoryOptions)
  }

  private fun getOrCreateWorker(
    paralellism: Int,
    workflowParalellism: Int,
    queueName: String,
  ): Worker {
    val worker = workerFactory.tryGetWorker(queueName)
    if (worker != null) {
      return worker
    }

    val workerOptions =
      WorkerOptions
        .newBuilder()
        .setMaxConcurrentActivityExecutionSize(paralellism)
        .setMaxConcurrentWorkflowTaskExecutionSize(workflowParalellism)
        .build()
    return workerFactory.newWorker(queueName, workerOptions).also {
      // Important to make sure we start in an off mode.
      it.suspendPolling()

      it.registerActivitiesImplementations(workloadStarterActivities)
      it.registerWorkflowImplementationTypes(temporalProxyHelper.proxyWorkflowClass(LauncherWorkflowImpl::class.java))
    }
  }
}
