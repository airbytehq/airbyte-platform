/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.book_keeping.events;

import io.airbyte.api.client.model.generated.StreamStatusIncompleteRunCause;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.workers.context.ReplicationContext;
import io.airbyte.workers.internal.book_keeping.AirbyteMessageOrigin;
import java.util.Optional;

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
public record ReplicationAirbyteMessageEvent(AirbyteMessageOrigin airbyteMessageOrigin,
                                             AirbyteMessage airbyteMessage,
                                             ReplicationContext replicationContext,
                                             Optional<StreamStatusIncompleteRunCause> incompleteRunCause) {

  public ReplicationAirbyteMessageEvent(final AirbyteMessageOrigin airbyteMessageOrigin,
                                        final AirbyteMessage airbyteMessage,
                                        final ReplicationContext replicationContext) {
    this(airbyteMessageOrigin, airbyteMessage, replicationContext, Optional.empty());
  }

}
