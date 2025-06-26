/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.util

import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger { }

/**
 * Utility class that provides helper functions for extracting data from a [AirbyteMessage].
 */
@Singleton
class AirbyteMessageDataExtractor {
  fun getStreamFromMessage(airbyteMessage: AirbyteMessage): StreamDescriptor? =
    when (airbyteMessage.type) {
      AirbyteMessage.Type.RECORD ->
        StreamDescriptor()
          .withName(airbyteMessage.record.stream)
          .withNamespace(airbyteMessage.record.namespace)

      AirbyteMessage.Type.STATE -> {
        logger.debug { "Extracting stream from state message: ${airbyteMessage.state.stream}" }
        if (airbyteMessage.state.stream != null) airbyteMessage.state.stream.streamDescriptor else null
      }

      AirbyteMessage.Type.TRACE -> getStreamFromTrace(airbyteMessage.trace)
      else -> null
    }
}

private fun getStreamFromTrace(airbyteTraceMessage: AirbyteTraceMessage): StreamDescriptor? =
  when (airbyteTraceMessage.type) {
    AirbyteTraceMessage.Type.STREAM_STATUS -> {
      logger.debug { "Extracting stream from stream status trace message: ${airbyteTraceMessage.streamStatus}" }
      airbyteTraceMessage.streamStatus.streamDescriptor
    }
    AirbyteTraceMessage.Type.ERROR -> {
      logger.debug { "Extracting stream from error trace message: ${airbyteTraceMessage.error}" }
      airbyteTraceMessage.getError().streamDescriptor
    }

    AirbyteTraceMessage.Type.ESTIMATE -> {
      logger.debug { "Extracting stream from estimate trace message: ${airbyteTraceMessage.estimate}" }
      StreamDescriptor()
        .withName(airbyteTraceMessage.estimate.name)
        .withNamespace(airbyteTraceMessage.estimate.namespace)
    }

    else -> null
  }
