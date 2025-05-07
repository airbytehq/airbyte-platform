/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.fixtures

import io.airbyte.config.WorkerDestinationConfig
import io.airbyte.container.orchestrator.worker.io.AirbyteDestination
import io.airbyte.protocol.models.v0.AirbyteMessage
import java.nio.file.Path
import java.util.Optional
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.Volatile

/**
 * Simple in-memory implementation of an AirbyteDestination for testing purpose.
 */
internal class SimpleAirbyteDestination : AirbyteDestination {
  private val messages: BlockingQueue<AirbyteMessage> = LinkedBlockingQueue()

  @Volatile
  private var finished = false

  @Throws(Exception::class)
  override fun start(
    destinationConfig: WorkerDestinationConfig,
    jobRoot: Path,
  ) {
  }

  @Throws(Exception::class)
  override fun accept(message: AirbyteMessage) {
    if (message.type == AirbyteMessage.Type.STATE) {
      messages.put(message)
    }
  }

  @Throws(Exception::class)
  override fun notifyEndOfInput() {
    finished = true
  }

  override val isFinished: Boolean
    get() = finished && messages.isEmpty()

  override val exitValue: Int
    get() = 0

  override fun attemptRead(): Optional<AirbyteMessage> {
    if (messages.isEmpty() && isFinished) {
      return Optional.empty<AirbyteMessage>()
    }
    return Optional.ofNullable<AirbyteMessage>(messages.poll())
  }

  @Throws(Exception::class)
  override fun close() {
  }

  @Throws(Exception::class)
  override fun cancel() {
  }
}
