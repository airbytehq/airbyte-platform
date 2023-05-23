/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.exception;

import io.airbyte.protocol.models.StreamDescriptor;
import java.io.Serial;

/**
 * Custom exception that represents a failure to transition a stream to a new status.
 */
public class StreamStatusException extends Exception {

  @Serial
  private static final long serialVersionUID = 2672916268471741528L;
  private final StreamDescriptor streamDescriptor;

  public StreamStatusException(final String message, final StreamDescriptor streamDescriptor) {
    super(message);
    this.streamDescriptor = streamDescriptor;
  }

  public StreamStatusException(final String message, final String streamName, final String streamNamespace) {
    this(message, new StreamDescriptor().withNamespace(streamNamespace).withName(streamName));
  }

  @Override
  public String getMessage() {
    return String.format("%s (stream = %s:%s)", super.getMessage(), streamDescriptor.getNamespace(), streamDescriptor.getName());
  }

}
