/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling.state;

import io.airbyte.commons.temporal.scheduling.state.listener.WorkflowStateChangedListener;
import io.airbyte.commons.temporal.scheduling.state.listener.WorkflowStateChangedListener.ChangedStateEvent;
import io.airbyte.commons.temporal.scheduling.state.listener.WorkflowStateChangedListener.StateField;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Contains the state of the currently running workflow execution in the connection manager
 * workflow.
 */
@Getter
@NoArgsConstructor
@ToString
public class WorkflowState {

  public WorkflowState(final UUID id, final WorkflowStateChangedListener stateChangedListener) {
    this.id = id;
    this.stateChangedListener = stateChangedListener;
  }

  @Setter
  private UUID id;
  @Setter
  private WorkflowStateChangedListener stateChangedListener;
  private boolean running = false;
  private boolean deleted = false;
  private boolean skipScheduling = false;
  private boolean updated = false;
  private boolean cancelled = false;
  private boolean failed = false;
  @Deprecated
  @Getter(AccessLevel.NONE)
  private final boolean resetConnection = false;
  @Deprecated
  @Getter(AccessLevel.NONE)
  private final boolean continueAsReset = false;
  @Deprecated
  @Getter(AccessLevel.NONE)
  private boolean quarantined = false;
  private boolean success = true;
  private boolean cancelledForReset = false;
  @Deprecated
  @Getter(AccessLevel.NONE)
  private final boolean resetWithScheduling = false;
  private boolean doneWaiting = false;
  private boolean skipSchedulingNextWorkflow = false;

  /**
   * Whether the CURRENT workflow has running.
   *
   * @param running true, if running.
   */
  public void setRunning(final boolean running) {
    final ChangedStateEvent event = new ChangedStateEvent(
        StateField.RUNNING,
        running);
    stateChangedListener.addEvent(id, event);
    this.running = running;
  }

  /**
   * Whether the workflow has been deleted.
   *
   * @param deleted true, if deleted.
   */
  public void setDeleted(final boolean deleted) {
    final ChangedStateEvent event = new ChangedStateEvent(
        StateField.DELETED,
        deleted);
    stateChangedListener.addEvent(id, event);
    this.deleted = deleted;
  }

  /**
   * Determines the scheduling behavior for the CURRENT run of the connection manager workflow. If set
   * to true, then the next sync should immediately execute and not wait for scheduling. If false,
   * then it will wait for the schedule as "normal".
   *
   * @param skipScheduling true, if should skip.
   */
  public void setSkipScheduling(final boolean skipScheduling) {
    final ChangedStateEvent event = new ChangedStateEvent(
        StateField.SKIPPED_SCHEDULING,
        skipScheduling);
    stateChangedListener.addEvent(id, event);
    this.skipScheduling = skipScheduling;
  }

  /**
   * Demarcates whether the connection's configurations have been updated during the current workflow
   * execution.
   *
   * @param updated true, if updated.
   */
  public void setUpdated(final boolean updated) {
    final ChangedStateEvent event = new ChangedStateEvent(
        StateField.UPDATED,
        updated);
    stateChangedListener.addEvent(id, event);
    this.updated = updated;
  }

  /**
   * Whether the CURRENT workflow has cancelled.
   *
   * @param cancelled true, if cancelled.
   */
  public void setCancelled(final boolean cancelled) {
    final ChangedStateEvent event = new ChangedStateEvent(
        StateField.CANCELLED,
        cancelled);
    stateChangedListener.addEvent(id, event);
    this.cancelled = cancelled;
  }

  /**
   * Whether the CURRENT workflow has failed.
   *
   * @param failed true, if failed.
   */
  public void setFailed(final boolean failed) {
    final ChangedStateEvent event = new ChangedStateEvent(
        StateField.FAILED,
        failed);
    stateChangedListener.addEvent(id, event);
    this.failed = failed;
  }

  /**
   * Whether the CURRENT workflow has succeeded.
   *
   * @param success true, if succeeded.
   */
  public void setSuccess(final boolean success) {
    final ChangedStateEvent event = new ChangedStateEvent(
        StateField.SUCCESS,
        success);
    stateChangedListener.addEvent(id, event);
    this.success = success;
  }

  /**
   * Whether the CURRENT workflow has been cancelled due to a reset request for the connection.
   *
   * @param cancelledForReset whether it is cancelled for reset
   */
  public void setCancelledForReset(final boolean cancelledForReset) {
    final ChangedStateEvent event = new ChangedStateEvent(
        StateField.CANCELLED_FOR_RESET,
        cancelledForReset);
    stateChangedListener.addEvent(id, event);
    this.cancelledForReset = cancelledForReset;
  }

  /**
   * Expresses what state the CURRENT workflow is in. If true, then waiting for the schedule to
   * trigger it. If false, it is not waiting for the schedule.
   *
   * @param doneWaiting whether it is waiting for the schedule
   */
  public void setDoneWaiting(final boolean doneWaiting) {
    final ChangedStateEvent event = new ChangedStateEvent(
        StateField.DONE_WAITING,
        doneWaiting);
    stateChangedListener.addEvent(id, event);
    this.doneWaiting = doneWaiting;
  }

  /**
   * Determines the scheduling behavior for the NEXT run of the connection manager workflow. If set to
   * true, the next run should immediately execute the sync and not wait for scheduling. If false,
   * then it will wait for the schedule as "normal".
   *
   * @param skipSchedulingNextWorkflow whether to wait for scheduling the next time or not
   */
  public void setSkipSchedulingNextWorkflow(final boolean skipSchedulingNextWorkflow) {
    final ChangedStateEvent event = new ChangedStateEvent(
        StateField.SKIP_SCHEDULING_NEXT_WORKFLOW,
        skipSchedulingNextWorkflow);
    stateChangedListener.addEvent(id, event);
    this.skipSchedulingNextWorkflow = skipSchedulingNextWorkflow;
  }

  /**
   * Reset the state of a workflow entirely.
   */
  // TODO: bmoric -> This is noisy when inpecting the list of event, it should be just a single reset
  // event.
  public void reset() {
    this.setRunning(false);
    this.setDeleted(false);
    this.setSkipScheduling(false);
    this.setUpdated(false);
    this.setCancelled(false);
    this.setFailed(false);
    this.setSuccess(false);
    this.setDoneWaiting(false);
    this.setSkipSchedulingNextWorkflow(false);
  }

}
