/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal;

import io.airbyte.config.WorkerSourceConfig;
import io.airbyte.protocol.models.AirbyteMessage;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;

/**
 * Simple in memory implementation of an AirbyteSource for testing purpose.
 */
public class SimpleAirbyteSource implements AirbyteSource {

  private final Queue<AirbyteMessage> messages = new LinkedList<>();
  private final List<AirbyteMessage> infiniteMessages = new ArrayList<>();

  /**
   * Configure the source to loop infinitely on the messages.
   */
  public void setInfiniteSourceWithMessages(final AirbyteMessage... messages) {
    this.infiniteMessages.clear();
    this.messages.clear();
    this.infiniteMessages.addAll(Arrays.stream(messages).toList());
  }

  /**
   * Configure the source to return all the messages then terminate.
   */
  public void setMessages(final AirbyteMessage... messages) {
    this.infiniteMessages.clear();
    this.messages.clear();
    this.messages.addAll(Arrays.stream(messages).toList());
  }

  @Override
  public void start(WorkerSourceConfig sourceConfig, Path jobRoot, UUID connectionId) throws Exception {

  }

  @Override
  public boolean isFinished() {
    return messages.isEmpty() && infiniteMessages.isEmpty();
  }

  @Override
  public int getExitValue() {
    return 0;
  }

  @Override
  public Optional<AirbyteMessage> attemptRead() {
    if (messages.isEmpty() && !infiniteMessages.isEmpty()) {
      this.messages.addAll(infiniteMessages);
    }

    if (!messages.isEmpty()) {
      return Optional.of(messages.poll());
    }
    return Optional.empty();
  }

  @Override
  public void close() throws Exception {

  }

  @Override
  public void cancel() throws Exception {

  }

}
