/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling.state.listener

import io.airbyte.commons.temporal.scheduling.state.listener.WorkflowStateChangedListener.ChangedStateEvent
import java.util.LinkedList
import java.util.Queue
import java.util.UUID

// todo (cgardens) what is this used for?

/**
 * State listener that does nothing. This is used in prod. The test case uses one that does
 * something so that we can test event changes. But in prod we don't want it to do anything.
 */
class NoopStateListener : WorkflowStateChangedListener {
  override fun events(testId: UUID): Queue<ChangedStateEvent> = LinkedList()

  override fun addEvent(
    id: UUID,
    event: ChangedStateEvent,
  ) {}
}
