/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping.events

import io.airbyte.api.client.model.generated.StreamStatusIncompleteRunCause
import io.airbyte.container.orchestrator.bookkeeping.AirbyteMessageOrigin
import io.airbyte.container.orchestrator.worker.context.ReplicationContext
import io.airbyte.protocol.models.v0.AirbyteMessage

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
