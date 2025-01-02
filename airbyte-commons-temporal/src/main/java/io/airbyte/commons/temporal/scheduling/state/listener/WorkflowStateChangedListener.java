/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling.state.listener;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.Objects;
import java.util.Queue;
import java.util.UUID;

/**
 * Listen for changes to the WorkflowState so that they can be communicated to a running
 * ConnectionManagerWorkflow.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
              include = JsonTypeInfo.As.PROPERTY,
              property = "type")
@JsonSubTypes({
  @Type(value = TestStateListener.class,
        name = "test"),
  @Type(value = NoopStateListener.class,
        name = "noop")
})
public interface WorkflowStateChangedListener {

  /**
   * WorkflowState field types.
   */
  enum StateField {
    CANCELLED,
    DELETED,
    RUNNING,
    SKIPPED_SCHEDULING,
    UPDATED,
    FAILED,
    RESET,
    CONTINUE_AS_RESET,
    SUCCESS,
    CANCELLED_FOR_RESET,
    RESET_WITH_SCHEDULING,
    DONE_WAITING,
    SKIP_SCHEDULING_NEXT_WORKFLOW,
  }

  /**
   * Container for transmitting changes to workflow state fields for a connection manager workflow.
   */
  class ChangedStateEvent {

    private final StateField field;
    private final boolean value;

    public ChangedStateEvent(StateField field, boolean value) {
      this.field = field;
      this.value = value;
    }

    public StateField getField() {
      return field;
    }

    public boolean isValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ChangedStateEvent that = (ChangedStateEvent) o;
      return value == that.value && field == that.field;
    }

    @Override
    public int hashCode() {
      return Objects.hash(field, value);
    }

    @Override
    public String toString() {
      return "ChangedStateEvent{field=" + field + ", value=" + value + '}';
    }

  }

  Queue<ChangedStateEvent> events(UUID testId);

  void addEvent(UUID testId, ChangedStateEvent event);

}
