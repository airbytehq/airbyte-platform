/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling.state.listener;

import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

/**
 * Workflow state change listener for testing. Used to verify the behavior of event signals in
 * testing.
 */
public class TestStateListener implements WorkflowStateChangedListener {

  private static final ConcurrentMap<UUID, Queue<ChangedStateEvent>> events = new ConcurrentHashMap<>();

  public static void reset() {
    events.clear();
  }

  @Override
  public Queue<ChangedStateEvent> events(final UUID testId) {
    return events.getOrDefault(testId, new ConcurrentLinkedQueue<>());
  }

  @Override
  public void addEvent(final UUID testId, final ChangedStateEvent event) {
    events.computeIfAbsent(testId, k -> new ConcurrentLinkedQueue<>()).add(event);

  }

}
