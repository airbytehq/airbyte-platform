/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling.state.listener;

import java.util.LinkedList;
import java.util.Queue;
import java.util.UUID;

/**
 * State listener that does nothing. This is used in prod. The test case uses one that does
 * something so that we can test event changes. But in prod we don't want it to do anything.
 */
// todo (cgardens) what is this used for?
public class NoopStateListener implements WorkflowStateChangedListener {

  @Override
  public Queue<ChangedStateEvent> events(final UUID id) {
    return new LinkedList<>();
  }

  @Override
  public void addEvent(final UUID id, final ChangedStateEvent event) {}

}
