/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal;

import io.airbyte.protocol.models.AirbyteMessage;
import java.io.IOException;

/**
 * Interface for writing airbyte messages. Base interface that the versioned writers build upon.
 */
public interface AirbyteMessageBufferedWriter {

  void write(AirbyteMessage message) throws IOException;

  void flush() throws IOException;

  void close() throws IOException;

}
