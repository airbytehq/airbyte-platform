package io.airbyte.workload.launcher

import io.airbyte.workload.launcher.mocks.LauncherInputMessage
import io.airbyte.workload.launcher.mocks.MessageConsumer
import io.airbyte.workload.launcher.pipeline.LaunchPipeline
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
class PipelineRunner(private val pipe: LaunchPipeline, private val consumer: MessageConsumer) {
  fun start() {
    // todo: replace with something smarter / stoppable via flag
    while (true) {
      val msg: LauncherInputMessage = consumer.read()
      pipe.accept(msg)
    }
  }
}
