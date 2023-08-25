/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling.state.listener;

import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Workflow state change listener for testing. Used to verify the behavior of event signals in
 * testing.
 */
public class TestStateListener implements WorkflowStateChangedListener {

  private static final ConcurrentHashMap<UUID, Queue<ChangedStateEvent>> events = new ConcurrentHashMap<>();

  public static void reset() {
    events.clear();
  }

  @Override
  public Queue<ChangedStateEvent> events(final UUID testId) {
    if (!events.containsKey(testId)) {
      return new ConcurrentLinkedQueue<>();
    }

    return events.get(testId);
  }

  @Override
  public void addEvent(final UUID testId, final ChangedStateEvent event) {
    Optional.ofNullable(events.get(testId))
        .or(() -> Optional.of(new LinkedList<>()))
        .stream()
        .forEach((eventQueue) -> {
          eventQueue.add(event);
          events.put(testId, eventQueue);
        });
  }

}
