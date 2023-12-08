package io.airbyte.workload.launcher.config

import io.airbyte.commons.temporal.TemporalConstants
import io.airbyte.commons.temporal.queue.QueueActivity
import io.airbyte.commons.temporal.queue.QueueActivityImpl
import io.airbyte.config.messages.LauncherInputMessage
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Geography
import io.airbyte.featureflag.WorkloadApiRouting
import io.airbyte.micronaut.temporal.TemporalProxyHelper
import io.airbyte.workload.launcher.pipeline.consumer.LauncherMessageConsumer
import io.airbyte.workload.launcher.pipeline.consumer.LauncherWorkflowImpl
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Property
import io.temporal.activity.ActivityOptions
import io.temporal.client.WorkflowClient
import io.temporal.opentracing.OpenTracingWorkerInterceptor
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
  fun specActivityOptions(
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

  @Singleton
  fun workloadStarterWorkerFactory(
    workflowClient: WorkflowClient,
    temporalProxyHelper: TemporalProxyHelper,
    @Named("workloadLauncherQueue") launcherQueue: String,
    @Named("starterActivities") workloadStarterActivities: QueueActivity<LauncherInputMessage>,
    @Property(name = "airbyte.workload-launcher.parallelism") paralellism: Int,
  ): WorkerFactory {
    val workerFactoryOptions =
      WorkerFactoryOptions.newBuilder()
        .setWorkerInterceptors(OpenTracingWorkerInterceptor())
        .build()
    val workerFactory = WorkerFactory.newInstance(workflowClient, workerFactoryOptions)
    val worker = workerFactory.newWorker(launcherQueue, WorkerOptions.newBuilder().setMaxConcurrentActivityExecutionSize(paralellism).build())
    worker.registerActivitiesImplementations(workloadStarterActivities)
    worker.registerWorkflowImplementationTypes(temporalProxyHelper.proxyWorkflowClass(LauncherWorkflowImpl::class.java))
    return workerFactory
  }

  @Named("workloadLauncherQueue")
  @Singleton
  fun launcherQueue(
    featureFlagClient: FeatureFlagClient,
    @Property(name = "airbyte.workload-launcher.geography") geography: String,
  ): String {
    return featureFlagClient.stringVariation(WorkloadApiRouting, Geography(geography))
  }
}
