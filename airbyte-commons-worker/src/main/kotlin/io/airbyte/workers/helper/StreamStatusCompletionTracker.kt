/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper

import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.protocol.models.AirbyteStreamStatusTraceMessage
import io.airbyte.protocol.models.AirbyteTraceMessage
import io.airbyte.protocol.models.StreamDescriptor
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.internal.AirbyteMapper
import jakarta.inject.Singleton
import java.time.Clock

@Singleton
class StreamStatusCompletionTracker(
  private val clock: Clock,
) {
  private val hasCompletedStatus = StreamStatusMap()
  private var shouldEmitStreamStatus = false

  fun startTracking(
    configuredAirbyteCatalog: ConfiguredAirbyteCatalog,
    supportRefreshes: Boolean,
  ) {
    shouldEmitStreamStatus = supportRefreshes

    if (shouldEmitStreamStatus) {
      configuredAirbyteCatalog.streams.forEach { stream ->
        hasCompletedStatus[StreamDescriptor().withName(stream.stream.name).withNamespace(stream.stream.namespace)] = false
      }
    }
  }

  fun track(streamStatus: AirbyteStreamStatusTraceMessage) {
    if (shouldEmitStreamStatus && streamStatus.status == AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.COMPLETE) {
      if (!hasCompletedStatus.containsStream(streamStatus.streamDescriptor)) {
        throw WorkerException(
          "A stream status (${streamStatus.streamDescriptor.namespace}.${streamStatus.streamDescriptor.name}) " +
            "has been detected for a stream not present in the catalog",
        )
      }
      hasCompletedStatus[streamStatus.streamDescriptor] = true
    }
  }

  fun finalize(
    exitCode: Int,
    namespacingMapper: AirbyteMapper,
  ): List<AirbyteMessage> {
    if (!shouldEmitStreamStatus || exitCode != 0) {
      return listOf()
    }
    return streamDescriptorsToCompleteStatusMessage(hasCompletedStatus.keys, namespacingMapper)
  }

  private fun streamDescriptorsToCompleteStatusMessage(
    streamDescriptors: Set<StreamDescriptor>,
    namespacingMapper: AirbyteMapper,
  ): List<AirbyteMessage> {
    return streamDescriptors.map {
      namespacingMapper.mapMessage(
        AirbyteMessage()
          .withType(AirbyteMessage.Type.TRACE)
          .withTrace(
            AirbyteTraceMessage()
              .withType(AirbyteTraceMessage.Type.STREAM_STATUS)
              .withEmittedAt(clock.millis().toDouble())
              .withStreamStatus(
                AirbyteStreamStatusTraceMessage()
                  .withStatus(AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.COMPLETE)
                  .withStreamDescriptor(it),
              ),
          ),
      )
    }
  }
}

/**
 * Custom map implementation that is used in order to override the `containsKey` functionality
 * to account for [StreamDescriptor] instances with either `null` or empty string `namespace`
 * values.
 */
class StreamStatusMap : HashMap<StreamDescriptor, Boolean>() {
  /**
   * Determines of the map contains an entry for the [StreamDescriptor].  It handles [StreamDescriptor] instances
   * with either a `null` or empty `namespace` value when checking for the value in the map.
   */
  fun containsStream(descriptor: StreamDescriptor): Boolean {
    return if (descriptor.namespace == null) {
      containsKey(descriptor) || containsKey(StreamDescriptor().withName(descriptor.name).withNamespace(""))
    } else {
      containsKey(descriptor)
    }
  }
}
