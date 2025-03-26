/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal;

import io.airbyte.config.WorkerDestinationConfig;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Simple in memory implementation of an AirbyteDestination for testing purpose.
 */
public class SimpleAirbyteDestination implements AirbyteDestination {

  private final BlockingQueue<AirbyteMessage> messages = new LinkedBlockingQueue<>();
  private volatile boolean isFinished = false;

  @Override
  public void start(WorkerDestinationConfig destinationConfig, Path jobRoot) throws Exception {}

  @Override
  public void accept(AirbyteMessage message) throws Exception {
    if (message.getType() == Type.STATE) {
      messages.put(message);
    }
  }

  @Override
  public void notifyEndOfInput() throws Exception {
    isFinished = true;
  }

  @Override
  public boolean isFinished() {
    return isFinished && messages.isEmpty();
  }

  @Override
  public int getExitValue() {
    return 0;
  }

  @Override
  public Optional<AirbyteMessage> attemptRead() {
    if (messages.isEmpty() && isFinished) {
      return Optional.empty();
    }
    return Optional.ofNullable(messages.poll());
  }

  @Override
  public void close() throws Exception {

  }

  @Override
  public void cancel() throws Exception {

  }

}
