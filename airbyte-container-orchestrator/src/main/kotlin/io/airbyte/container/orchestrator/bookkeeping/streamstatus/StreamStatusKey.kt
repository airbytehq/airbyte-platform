/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping.streamstatus

import io.airbyte.protocol.models.v0.StreamDescriptor

data class StreamStatusKey(
  var streamNamespace: String?,
  var streamName: String,
) {
  fun toDisplayName(): String =
    if (streamNamespace == null) {
      streamName
    } else {
      "$streamNamespace:$streamName"
    }

  companion object {
    fun fromProtocol(streamDesc: StreamDescriptor) =
      StreamStatusKey(
        streamNamespace = streamDesc.namespace,
        streamName = streamDesc.name,
      )
  }
}
