/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.book_keeping.events;

import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.workers.context.ReplicationContext;
import io.airbyte.workers.internal.book_keeping.AirbyteMessageOrigin;

/**
 * Custom event type that contains information about the current {@link AirbyteMessage} being
 * processed by replication.
 *
 * @param airbyteMessageOrigin The message origin of the associated {@link AirbyteMessage}.
 * @param airbyteMessage The Airbyte Protocol {@link AirbyteMessage}.
 * @param replicationContext Additional context about the replication process that produced the
 *        message.
 */
public record ReplicationAirbyteMessageEvent(AirbyteMessageOrigin airbyteMessageOrigin,
                                             AirbyteMessage airbyteMessage,
                                             ReplicationContext replicationContext) {}
