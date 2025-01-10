package io.airbyte.workload.launcher.config

import io.airbyte.commons.temporal.TemporalConstants
import io.airbyte.commons.temporal.queue.QueueActivity
import io.airbyte.commons.temporal.queue.QueueActivityImpl
import io.airbyte.config.messages.LauncherInputMessage
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Geography
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.PlaneName
import io.airbyte.featureflag.Priority
import io.airbyte.featureflag.Priority.Companion.HIGH_PRIORITY
import io.airbyte.featureflag.WorkloadApiRouting
import io.airbyte.micronaut.temporal.TemporalProxyHelper
import io.airbyte.workload.launcher.pipeline.consumer.LauncherMessageConsumer
import io.airbyte.workload.launcher.pipeline.consumer.LauncherWorkflowImpl
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Property
import io.temporal.activity.ActivityOptions
import io.temporal.client.WorkflowClient
import io.temporal.opentracing.OpenTracingWorkerInterceptor
import io.temporal.worker.Worker
import io.temporal.worker.WorkerFactory
import io.temporal.worker.WorkerFactoryOptions
import io.temporal.worker.WorkerOptions
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.Duration

@Factory
class TemporalBeanFactory {
  @Singleton
  @Named("queueActivityOptions")
  fun queueActivityOptions(
    @Property(name = "airbyte.workload-launcher.workload-start-timeout") workloadStartTimeout: Duration,
  ): ActivityOptions {
    return ActivityOptions.newBuilder()
      .setScheduleToCloseTimeout(workloadStartTimeout)
      .setRetryOptions(TemporalConstants.NO_RETRY)
      .build()
  }

  @Singleton
  @Named("starterActivities")
  fun workloadStarterActivities(launcherMessageConsumer: LauncherMessageConsumer): QueueActivity<LauncherInputMessage> {
    return QueueActivityImpl(launcherMessageConsumer)
  }

  @Named("workerFactory")
  @Singleton
  fun workloadStarterWorkerFactory(
    workflowClient: WorkflowClient,
    temporalProxyHelper: TemporalProxyHelper,
    @Named("workloadLauncherQueue") launcherQueue: String,
    @Named("starterActivities") workloadStarterActivities: QueueActivity<LauncherInputMessage>,
    @Property(name = "airbyte.workload-launcher.temporal.default-queue.parallelism") paralellism: Int,
    @Property(name = "airbyte.workload-launcher.temporal.default-queue.workflow-parallelism") workflowParalellism: Int,
  ): WorkerFactory {
    val workerFactory = baseWorkerFactory(workflowClient)
    val worker = baseWorker(workerFactory, paralellism, workflowParalellism, launcherQueue)
    worker.registerActivitiesImplementations(workloadStarterActivities)
    worker.registerWorkflowImplementationTypes(temporalProxyHelper.proxyWorkflowClass(LauncherWorkflowImpl::class.java))
    return workerFactory
  }

  @Named("highPriorityWorkerFactory")
  @Singleton
  fun workloadStarterHighPriorityWorkerFactory(
    workflowClient: WorkflowClient,
    temporalProxyHelper: TemporalProxyHelper,
    @Named("workloadLauncherHighPriorityQueue") launcherQueue: String,
    @Named("starterActivities") workloadStarterActivities: QueueActivity<LauncherInputMessage>,
    @Property(name = "airbyte.workload-launcher.temporal.high-priority-queue.parallelism") paralellism: Int,
    @Property(name = "airbyte.workload-launcher.temporal.high-priority-queue.workflow-parallelism") workflowParalellism: Int,
  ): WorkerFactory {
    val workerFactory = baseWorkerFactory(workflowClient)
    val worker = baseWorker(workerFactory, paralellism, workflowParalellism, launcherQueue)
    worker.registerActivitiesImplementations(workloadStarterActivities)
    worker.registerWorkflowImplementationTypes(temporalProxyHelper.proxyWorkflowClass(LauncherWorkflowImpl::class.java))
    return workerFactory
  }

  @Named("workloadLauncherQueue")
  @Singleton
  fun launcherQueue(
    featureFlagClient: FeatureFlagClient,
    @Property(name = "airbyte.workload-launcher.geography") geography: String,
    @Property(name = "airbyte.data-plane-name") dataPlaneName: String?,
  ): String {
    val context =
      if (dataPlaneName.isNullOrBlank()) {
        Geography(geography)
      } else {
        Multi(listOf(Geography(geography), PlaneName(dataPlaneName)))
      }
    return featureFlagClient.stringVariation(WorkloadApiRouting, context)
  }

  @Named("workloadLauncherHighPriorityQueue")
  @Singleton
  fun highPriorityLauncherQueue(
    featureFlagClient: FeatureFlagClient,
    @Property(name = "airbyte.workload-launcher.geography") geography: String,
    @Property(name = "airbyte.data-plane-name") dataPlaneName: String?,
  ): String {
    val context =
      if (dataPlaneName.isNullOrBlank()) {
        Multi(listOf(Geography(geography), Priority(HIGH_PRIORITY)))
      } else {
        Multi(listOf(Geography(geography), Priority(HIGH_PRIORITY), PlaneName(dataPlaneName)))
      }
    return featureFlagClient.stringVariation(WorkloadApiRouting, context)
  }

  private fun baseWorkerFactory(workflowClient: WorkflowClient): WorkerFactory {
    val workerFactoryOptions =
      WorkerFactoryOptions.newBuilder()
        .setWorkerInterceptors(OpenTracingWorkerInterceptor())
        .build()
    return WorkerFactory.newInstance(workflowClient, workerFactoryOptions)
  }

  private fun baseWorker(
    workerFactory: WorkerFactory,
    paralellism: Int,
    workflowParalellism: Int,
    queueName: String,
  ): Worker {
    val workerOptions =
      WorkerOptions.newBuilder()
        .setMaxConcurrentActivityExecutionSize(paralellism)
        .setMaxConcurrentWorkflowTaskExecutionSize(workflowParalellism)
        .build()
    return workerFactory.newWorker(queueName, workerOptions)
  }
}
