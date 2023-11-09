package io.airbyte.workload.api

import io.airbyte.commons.temporal.queue.TemporalMessageProducer
import io.airbyte.config.messages.LauncherInputMessage
import jakarta.inject.Singleton

/**
 * Placeholder class to interact with the launcher queue for testing.
 * Should be merged with the controller when ready.
 */
@Singleton
open class WorkloadService(private val messageProducer: TemporalMessageProducer<LauncherInputMessage>) {
  open fun create(
    workloadId: String,
    workloadInput: String,
  ) {
    messageProducer.publish("launcher-queue", LauncherInputMessage(workloadId, workloadInput))
  }
}
