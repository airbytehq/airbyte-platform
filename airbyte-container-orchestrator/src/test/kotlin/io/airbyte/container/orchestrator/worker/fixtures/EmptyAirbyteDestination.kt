/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.fixtures

import io.airbyte.config.WorkerDestinationConfig
import io.airbyte.container.orchestrator.worker.io.AirbyteDestination
import io.airbyte.protocol.models.v0.AirbyteMessage
import java.nio.file.Path
import java.util.Optional
import kotlin.concurrent.Volatile

/**
 * Empty Airbyte Destination. Does nothing with messages. Intended for performance testing.
 */
internal class EmptyAirbyteDestination : AirbyteDestination {
  @Volatile
  private var finished = false

  override fun start(
    destinationConfig: WorkerDestinationConfig,
    jobRoot: Path,
  ) {
    finished = false
  }

  override fun accept(message: AirbyteMessage) {
  }

  override fun notifyEndOfInput() {
    finished = true
  }

  override val isFinished: Boolean
    get() = finished

  override val exitValue: Int
    get() = 0

  override fun attemptRead(): Optional<AirbyteMessage> = Optional.empty<AirbyteMessage>()

  override fun close() {
  }

  override fun cancel() {
  }
}
