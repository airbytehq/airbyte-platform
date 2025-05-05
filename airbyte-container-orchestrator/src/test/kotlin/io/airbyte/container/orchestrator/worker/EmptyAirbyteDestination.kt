/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import io.airbyte.config.WorkerDestinationConfig
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.workers.internal.AirbyteDestination
import java.nio.file.Path
import java.util.Optional
import kotlin.concurrent.Volatile

/**
 * Empty Airbyte Destination. Does nothing with messages. Intended for performance testing.
 */
class EmptyAirbyteDestination : AirbyteDestination {
  @Volatile
  private var isFinished = false

  @Throws(Exception::class)
  override fun start(
    destinationConfig: WorkerDestinationConfig,
    jobRoot: Path?,
  ) {
    isFinished = false
  }

  @Throws(Exception::class)
  override fun accept(message: AirbyteMessage) {
  }

  @Throws(Exception::class)
  override fun notifyEndOfInput() {
    isFinished = true
  }

  override fun isFinished(): Boolean = isFinished

  override fun getExitValue(): Int = 0

  override fun attemptRead(): Optional<AirbyteMessage> = Optional.empty<AirbyteMessage>()

  @Throws(Exception::class)
  override fun close() {
  }

  @Throws(Exception::class)
  override fun cancel() {
  }
}
