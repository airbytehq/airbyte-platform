/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import io.airbyte.config.WorkerDestinationConfig
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.workers.internal.AirbyteDestination
import java.nio.file.Path
import java.util.Optional
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.Volatile

/**
 * Simple in-memory implementation of an AirbyteDestination for testing purpose.
 */
class SimpleAirbyteDestination : AirbyteDestination {
  private val messages: BlockingQueue<AirbyteMessage> = LinkedBlockingQueue()

  @Volatile
  private var isFinished = false

  @Throws(Exception::class)
  override fun start(
    destinationConfig: WorkerDestinationConfig,
    jobRoot: Path,
  ) {
  }

  @Throws(Exception::class)
  override fun accept(message: AirbyteMessage) {
    if (message.getType() == AirbyteMessage.Type.STATE) {
      messages.put(message)
    }
  }

  @Throws(Exception::class)
  override fun notifyEndOfInput() {
    isFinished = true
  }

  override fun isFinished(): Boolean = isFinished && messages.isEmpty()

  override fun getExitValue(): Int = 0

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
