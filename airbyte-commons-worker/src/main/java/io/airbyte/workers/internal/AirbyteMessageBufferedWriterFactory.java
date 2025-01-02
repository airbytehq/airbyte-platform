/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal;

import java.io.BufferedWriter;

/**
 * Factory for creating airbyte message writers. Base class that the versioned writers build upon.
 */
public interface AirbyteMessageBufferedWriterFactory {

  AirbyteMessageBufferedWriter createWriter(BufferedWriter bufferedWriter);

}
