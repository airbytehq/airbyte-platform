/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling.state.listener

import io.airbyte.commons.temporal.scheduling.state.listener.WorkflowStateChangedListener.ChangedStateEvent
import java.util.Queue
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ConcurrentMap

/**
 * Workflow state change listener for testing. Used to verify the behavior of event signals in
 * testing.
 */
class TestStateListener : WorkflowStateChangedListener {
  override fun events(testId: UUID): Queue<ChangedStateEvent> = events.getOrDefault(testId, ConcurrentLinkedQueue())

  override fun addEvent(
    testId: UUID,
    event: ChangedStateEvent,
  ) {
    events.computeIfAbsent(testId) { k: UUID? -> ConcurrentLinkedQueue() }.add(event)
  }

  companion object {
    private val events: ConcurrentMap<UUID, Queue<ChangedStateEvent>> = ConcurrentHashMap()

    @JvmStatic
    fun reset() {
      events.clear()
    }
  }
}
