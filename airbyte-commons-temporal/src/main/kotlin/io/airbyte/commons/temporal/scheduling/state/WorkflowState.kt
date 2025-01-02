/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling.state

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import io.airbyte.commons.temporal.scheduling.state.listener.WorkflowStateChangedListener
import io.airbyte.commons.temporal.scheduling.state.listener.WorkflowStateChangedListener.ChangedStateEvent
import io.airbyte.commons.temporal.scheduling.state.listener.WorkflowStateChangedListener.StateField
import java.util.UUID

/**
 * Contains the state of the currently running workflow execution in the connection manager workflow.
 */
class WorkflowState
  @JsonCreator
  constructor(
    @JsonProperty("id") var id: UUID,
    @JsonProperty("stateChangedListener") var stateChangedListener: WorkflowStateChangedListener,
  ) {
    var isRunning = false
      /** Whether the CURRENT workflow has running. */
      set(value) {
        val event = ChangedStateEvent(StateField.RUNNING, value)
        stateChangedListener.addEvent(id, event)
        field = value
      }

    /** Whether the workflow has been deleted. */
    var isDeleted = false
      set(value) {
        val event = ChangedStateEvent(StateField.DELETED, value)
        stateChangedListener.addEvent(id, event)
        field = value
      }

    /**
     * Determines the scheduling behavior for the CURRENT run of the connection manager workflow. If set
     * to true, then the next sync should immediately execute and not wait for scheduling. If false,
     * then it will wait for the schedule as "normal".
     *
     */
    var isSkipScheduling = false
      set(value) {
        val event = ChangedStateEvent(StateField.SKIPPED_SCHEDULING, value)
        stateChangedListener.addEvent(id, event)
        field = value
      }

    /**
     * Demarcates whether the connection's configurations have been updated during the current workflow
     * execution.
     */
    var isUpdated = false
      set(value) {
        val event = ChangedStateEvent(StateField.UPDATED, value)
        stateChangedListener.addEvent(id, event)
        field = value
      }

    /** Whether the CURRENT workflow has cancelled. */
    var isCancelled = false
      set(value) {
        val event = ChangedStateEvent(StateField.CANCELLED, value)
        stateChangedListener.addEvent(id, event)
        field = value
      }

    /** Whether the CURRENT workflow has failed. */
    var isFailed = false
      set(value) {
        val event = ChangedStateEvent(StateField.FAILED, value)
        stateChangedListener.addEvent(id, event)
        field = value
      }

    /** Whether the CURRENT workflow has succeeded. */
    var isSuccess = false
      set(value) {
        val event = ChangedStateEvent(StateField.SUCCESS, value)
        stateChangedListener.addEvent(id, event)
        field = value
      }

    /** Whether the CURRENT workflow has been cancelled due to a reset request for the connection. */
    var isCancelledForReset = false
      set(value) {
        val event = ChangedStateEvent(StateField.CANCELLED_FOR_RESET, value)
        stateChangedListener.addEvent(id, event)
        field = value
      }

    /**
     * Expresses what state the CURRENT workflow is in. If true, then waiting for the schedule to
     * trigger it. If false, it is not waiting for the schedule.
     */
    var isDoneWaiting = false
      set(value) {
        val event = ChangedStateEvent(StateField.DONE_WAITING, value)
        stateChangedListener.addEvent(id, event)
        field = value
      }

    /**
     * Determines the scheduling behavior for the NEXT run of the connection manager workflow. If set to
     * true, the next run should immediately execute the sync and not wait for scheduling. If false,
     * then it will wait for the schedule as "normal".
     */
    var isSkipSchedulingNextWorkflow = false
      set(value) {
        val event = ChangedStateEvent(StateField.SKIP_SCHEDULING_NEXT_WORKFLOW, value)
        stateChangedListener.addEvent(id, event)
        field = value
      }

    /**
     * Reset the state of a workflow entirely.
     * TODO: bmoric -> This is noisy when inpecting the list of event, it should be just a single reset event.
     */
    fun reset() {
      isRunning = false
      isDeleted = false
      isSkipScheduling = false
      isUpdated = false
      isCancelled = false
      isFailed = false
      isSuccess = false
      isDoneWaiting = false
      isSkipSchedulingNextWorkflow = false
    }
  }
