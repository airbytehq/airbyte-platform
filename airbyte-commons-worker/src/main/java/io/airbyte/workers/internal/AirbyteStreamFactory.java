/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal;

import io.airbyte.protocol.models.AirbyteMessage;
import java.io.BufferedReader;
import java.util.stream.Stream;

/**
 * Interface for creating an AirbyteStream from an InputStream.
 */
public interface AirbyteStreamFactory {

  Stream<AirbyteMessage> create(BufferedReader bufferedReader);

}
