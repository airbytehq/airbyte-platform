package io.airbyte.workload.launcher.pipeline

import io.airbyte.commons.temporal.queue.MessageConsumer
import io.airbyte.config.messages.LauncherInputMessage
import jakarta.inject.Singleton

@Singleton
class LauncherMessageConsumer(private val launchPipeline: LaunchPipeline) : MessageConsumer<LauncherInputMessage> {
  override fun consume(input: LauncherInputMessage) {
    launchPipeline.accept(
      LauncherInput(
        workloadId = input.workloadId,
        workloadInput = input.workloadInput,
      ),
    )
  }
}
