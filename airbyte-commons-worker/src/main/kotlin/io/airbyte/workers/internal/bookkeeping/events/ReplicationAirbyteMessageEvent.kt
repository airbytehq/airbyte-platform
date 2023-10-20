package io.airbyte.workers.internal.bookkeeping.events

import io.airbyte.api.client.model.generated.StreamStatusIncompleteRunCause
import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.workers.context.ReplicationContext
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageOrigin

/**
 * Custom event type that contains information about the current {@link AirbyteMessage} being
 * processed by replication.
 *
 * @param airbyteMessageOrigin The message origin of the associated {@link AirbyteMessage}.
 * @param airbyteMessage The Airbyte Protocol {@link AirbyteMessage}.
 * @param replicationContext Additional context about the replication process that produced the
 *        message.
 * @param incompleteRunCause The optional incomplete status run cause.
 */
data class ReplicationAirbyteMessageEvent
  @JvmOverloads
  constructor(
    val airbyteMessageOrigin: AirbyteMessageOrigin,
    val airbyteMessage: AirbyteMessage,
    val replicationContext: ReplicationContext,
    val incompleteRunCause: StreamStatusIncompleteRunCause? = null,
  )
