/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.fixtures

import io.airbyte.config.WorkerSourceConfig
import io.airbyte.container.orchestrator.worker.io.AirbyteSource
import io.airbyte.protocol.models.v0.AirbyteMessage
import java.nio.file.Path
import java.util.LinkedList
import java.util.Optional
import java.util.Queue
import java.util.UUID

/**
 * Simple in-memory implementation of an AirbyteSource for testing purpose.
 */
internal class SimpleAirbyteSource : AirbyteSource {
  private val messages: Queue<AirbyteMessage> = LinkedList()
  private val infiniteMessages: MutableList<AirbyteMessage?> = mutableListOf()

  /**
   * Configure the source to loop infinitely on the messages.
   */
  fun setInfiniteSourceWithMessages(vararg messages: AirbyteMessage) {
    this.infiniteMessages.clear()
    this.messages.clear()
    this.infiniteMessages.addAll(messages)
  }

  /**
   * Configure the source to return all the messages then terminate.
   */
  fun setMessages(vararg messages: AirbyteMessage) {
    this.infiniteMessages.clear()
    this.messages.clear()
    this.messages.addAll(messages)
  }

  override fun start(
    sourceConfig: WorkerSourceConfig,
    jobRoot: Path?,
    connectionId: UUID?,
  ) {
  }

  override val isFinished: Boolean
    get() = messages.isEmpty() && infiniteMessages.isEmpty()

  override val exitValue: Int
    get() = 0

  override fun attemptRead(): Optional<AirbyteMessage> {
    if (messages.isEmpty() && !infiniteMessages.isEmpty()) {
      this.messages.addAll(infiniteMessages)
    }

    if (!messages.isEmpty()) {
      return Optional.of<AirbyteMessage>(messages.poll()!!)
    }
    return Optional.empty<AirbyteMessage>()
  }

  override fun close() {
  }

  override fun cancel() {
  }
}
