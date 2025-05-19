/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.model

import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import java.util.concurrent.atomic.AtomicInteger

const val ID = "id"

fun attachIdToStateMessageFromSource(message: AirbyteMessage): AirbyteMessage {
  if (message.type == AirbyteMessage.Type.STATE) {
    attachIdToStateMessageFromSource(message.state)
  }

  return message
}

fun attachIdToStateMessageFromSource(message: AirbyteStateMessage): AirbyteStateMessage =
  with(message) {
    if (!message.additionalProperties.contains(ID)) {
      setAdditionalProperty(ID, StateIdProvider.nextId)
    }
    return this
  }

fun getIdFromStateMessage(message: AirbyteMessage): Int? {
  if (message.type == AirbyteMessage.Type.STATE) {
    return getIdFromStateMessage(message.state)
  }
  return null
}

fun getIdFromStateMessage(message: AirbyteStateMessage): Int =
  if (message.additionalProperties.contains(
      ID,
    )
  ) {
    message.additionalProperties[ID] as Int
  } else {
    throw IllegalStateException("State message does not contain id")
  }

private object StateIdProvider {
  private val id = AtomicInteger(0)

  val nextId: Int
    get() = id.incrementAndGet()
}
