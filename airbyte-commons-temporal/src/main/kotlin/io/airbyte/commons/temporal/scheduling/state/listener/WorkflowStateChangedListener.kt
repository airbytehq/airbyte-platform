/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling.state.listener

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import java.util.Objects
import java.util.Queue
import java.util.UUID

/**
 * Listen for changes to the WorkflowState so that they can be communicated to a running
 * ConnectionManagerWorkflow.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(JsonSubTypes.Type(value = TestStateListener::class, name = "test"), JsonSubTypes.Type(value = NoopStateListener::class, name = "noop"))
interface WorkflowStateChangedListener {
  /**
   * WorkflowState field types.
   */
  enum class StateField {
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
  class ChangedStateEvent(
    @JvmField val field: StateField,
    val isValue: Boolean,
  ) {
    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as ChangedStateEvent
      return isValue == that.isValue && field == that.field
    }

    override fun hashCode(): Int = Objects.hash(field, isValue)

    override fun toString(): String = "ChangedStateEvent{field=" + field + ", value=" + isValue + '}'
  }

  fun events(testId: UUID): Queue<ChangedStateEvent>

  fun addEvent(
    testId: UUID,
    event: ChangedStateEvent,
  )
}
