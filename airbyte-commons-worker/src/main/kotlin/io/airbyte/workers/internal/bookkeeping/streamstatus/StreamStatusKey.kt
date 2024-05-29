package io.airbyte.workers.internal.bookkeeping.streamstatus

import io.airbyte.protocol.models.StreamDescriptor

data class StreamStatusKey(
  var streamNamespace: String?,
  var streamName: String,
) {
  fun toDisplayName(): String {
    return if (streamNamespace == null) {
      streamName
    } else {
      "$streamNamespace:$streamName"
    }
  }

  companion object {
    fun fromProtocol(streamDesc: StreamDescriptor) = StreamStatusKey(streamDesc.namespace, streamDesc.name)
  }
}
