/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping.state

import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.SyncMode
import io.airbyte.config.helpers.ProtocolConverters.Companion.toInternal
import io.airbyte.config.helpers.ProtocolConverters.Companion.toProtocol
import io.airbyte.container.orchestrator.worker.ReplicationContextProvider
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStreamState

/**
 * Injects empty state messages at the end of full refresh streams if the source didn't send any
 * state messages for them.
 *
 * This works around older sources implementations that don't send state messages for full refresh
 * streams. This enables extracting some information from the destinations that are only sent on the StateStats
 * part of the state messages.
 */
class MissingStateInjector(
  replicationContext: ReplicationContextProvider.Context,
) {
  private var isGlobal = false
  private val streamToStateSeen: MutableMap<StreamDescriptor, Boolean> =
    replicationContext
      .configuredCatalog
      .streams
      .filter { shouldConsiderFillingStates(it) }
      .associate { it.streamDescriptor to false }
      .toMutableMap()

  fun trackMessage(message: AirbyteMessage) {
    if (message.type != AirbyteMessage.Type.STATE) {
      return
    }

    if (message.state.type == AirbyteStateMessage.AirbyteStateType.STREAM) {
      streamToStateSeen[
        message.state.stream.streamDescriptor
          .toInternal(),
      ] = true
    } else {
      isGlobal = true
    }
  }

  fun getStatesToInject(): List<AirbyteMessage> {
    if (isGlobal) {
      return emptyList()
    }
    return streamToStateSeen
      .filter { !it.value }
      .keys
      .map { getEmptyStateMessage(it) }
  }

  private fun getEmptyStateMessage(sd: StreamDescriptor): AirbyteMessage =
    AirbyteMessage()
      .withType(AirbyteMessage.Type.STATE)
      .withState(
        AirbyteStateMessage()
          .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
          .withStream(
            AirbyteStreamState()
              .withStreamDescriptor(sd.toProtocol())
              .withStreamState(Jsons.emptyObject()),
          ),
      )

  private fun shouldConsiderFillingStates(stream: ConfiguredAirbyteStream): Boolean =
    stream.syncMode == SyncMode.FULL_REFRESH && !(stream.stream.isResumable ?: false)
}
