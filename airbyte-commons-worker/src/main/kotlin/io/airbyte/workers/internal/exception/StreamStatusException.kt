/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.exception

import io.airbyte.protocol.models.StreamDescriptor
import io.airbyte.workers.context.ReplicationContext
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageOrigin

/**
 * Custom exception that represents a failure to transition a stream to a new status.
 */
class StreamStatusException(
  message: String,
  private val airbyteMessageOrigin: AirbyteMessageOrigin,
  private val replicationContext: ReplicationContext,
  private val streamDescriptor: StreamDescriptor,
) : Exception(message) {
  constructor(
    message: String,
    airbyteMessageOrigin: AirbyteMessageOrigin,
    replicationContext: ReplicationContext,
    streamName: String,
    streamNamespace: String?,
  ) : this(message, airbyteMessageOrigin, replicationContext, StreamDescriptor().withNamespace(streamNamespace).withName(streamName))

  override val message: String
    get() =
      "$message (origin = ${airbyteMessageOrigin.name}, context = $replicationContext" +
        ", stream = ${streamDescriptor.namespace}:${streamDescriptor.name})"
}
