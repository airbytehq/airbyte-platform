/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.exception;

import io.airbyte.protocol.models.StreamDescriptor;
import io.airbyte.workers.context.ReplicationContext;
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageOrigin;
import java.io.Serial;

/**
 * Custom exception that represents a failure to transition a stream to a new status.
 */
@SuppressWarnings("PMD.NonSerializableClass")
public class StreamStatusException extends Exception {

  @Serial
  private static final long serialVersionUID = 2672916268471741528L;
  private final AirbyteMessageOrigin airbyteMessageOrigin;
  private final ReplicationContext replicationContext;
  private final StreamDescriptor streamDescriptor;

  public StreamStatusException(final String message,
                               final AirbyteMessageOrigin airbyteMessageOrigin,
                               final ReplicationContext replicationContext,
                               final StreamDescriptor streamDescriptor) {
    super(message);
    this.airbyteMessageOrigin = airbyteMessageOrigin;
    this.replicationContext = replicationContext;
    this.streamDescriptor = streamDescriptor;
  }

  public StreamStatusException(final String message,
                               final AirbyteMessageOrigin airbyteMessageOrigin,
                               final ReplicationContext replicationContext,
                               final String streamName,
                               final String streamNamespace) {
    this(message, airbyteMessageOrigin, replicationContext, new StreamDescriptor().withNamespace(streamNamespace).withName(streamName));
  }

  @Override
  public String getMessage() {
    return String.format("%s (origin = %s, context = %s, stream = %s:%s)", super.getMessage(), airbyteMessageOrigin.name(), replicationContext,
        streamDescriptor.getNamespace(), streamDescriptor.getName());
  }

}
