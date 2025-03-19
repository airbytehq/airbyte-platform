/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import io.airbyte.config.WorkerDestinationConfig;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.workers.internal.AirbyteDestination;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Empty Airbyte Destination. Does nothing with messages. Intended for performance testing.
 */
public class EmptyAirbyteDestination implements AirbyteDestination {

  private volatile boolean isFinished;

  @Override
  public void start(WorkerDestinationConfig destinationConfig, Path jobRoot) throws Exception {
    isFinished = false;
  }

  @Override
  public void accept(AirbyteMessage message) throws Exception {

  }

  @Override
  public void notifyEndOfInput() throws Exception {
    isFinished = true;
  }

  @Override
  public boolean isFinished() {
    return isFinished;
  }

  @Override
  public int getExitValue() {
    return 0;
  }

  @Override
  public Optional<AirbyteMessage> attemptRead() {
    return Optional.empty();
  }

  @Override
  public void close() throws Exception {}

  @Override
  public void cancel() throws Exception {

  }

}
