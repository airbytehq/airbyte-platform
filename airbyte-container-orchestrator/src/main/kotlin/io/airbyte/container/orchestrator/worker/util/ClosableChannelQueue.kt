/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.util

import kotlinx.coroutines.channels.Channel

class ClosableChannelQueue<T>(
  capacity: Int,
) {
  private val channel = Channel<T>(capacity)

  suspend fun receive(): T? = channel.receiveCatching().getOrNull()

  suspend fun send(e: T) = channel.send(e)

  fun close() {
    channel.close()
  }

  fun isClosedForSending(): Boolean = channel.isClosedForSend

  fun isClosedForReceiving(): Boolean = channel.isClosedForReceive
}
