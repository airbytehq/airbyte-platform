package io.airbyte.workload.launcher.config

import io.airbyte.commons.temporal.TemporalConstants
import io.airbyte.commons.temporal.queue.QueueActivity
import io.airbyte.commons.temporal.queue.QueueActivityImpl
import io.airbyte.config.messages.LauncherInputMessage
import io.airbyte.micronaut.temporal.TemporalProxyHelper
import io.airbyte.workload.launcher.pipeline.consumer.LauncherMessageConsumer
import io.airbyte.workload.launcher.pipeline.consumer.LauncherWorkflowImpl
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Property
import io.temporal.activity.ActivityOptions
import io.temporal.client.WorkflowClient
import io.temporal.worker.WorkerFactory
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
    @Property(name = "airbyte.workload-launcher.queue") starterQueue: String,
    @Named("starterActivities") workloadStarterActivities: QueueActivity<LauncherInputMessage>,
    @Property(name = "airbyte.workload-launcher.parallelism") paralellism: Int,
  ): WorkerFactory {
    val workerFactory = WorkerFactory.newInstance(workflowClient)
    val worker = workerFactory.newWorker(starterQueue, WorkerOptions.newBuilder().setMaxConcurrentActivityExecutionSize(paralellism).build())
    worker.registerActivitiesImplementations(workloadStarterActivities)
    worker.registerWorkflowImplementationTypes(temporalProxyHelper.proxyWorkflowClass(LauncherWorkflowImpl::class.java))
    return workerFactory
  }
}
