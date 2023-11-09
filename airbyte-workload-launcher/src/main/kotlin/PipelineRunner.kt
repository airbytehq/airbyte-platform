package io.airbyte.workload.launcher

import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.metrics.WorkloadLauncherMetricMetadata
import io.airbyte.workload.launcher.mocks.MessageConsumer
import io.airbyte.workload.launcher.pipeline.LaunchPipeline
import io.airbyte.workload.launcher.pipeline.LauncherInput
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
class PipelineRunner(
  private val pipe: LaunchPipeline,
  private val consumer: MessageConsumer,
  private val metricPublisher: CustomMetricPublisher,
) {
  fun start() {
    metricPublisher.gauge(
      WorkloadLauncherMetricMetadata.WORKLOAD_QUEUE_SIZE,
      consumer,
      { messageConsumer -> messageConsumer.size().toDouble() },
    )
    // todo: replace with something smarter / stoppable via flag
    while (true) {
      val msg: LauncherInput = consumer.read()
      pipe.accept(msg)
    }
  }
}
