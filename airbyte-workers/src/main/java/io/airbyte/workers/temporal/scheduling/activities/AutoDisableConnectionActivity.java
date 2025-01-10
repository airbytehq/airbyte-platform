/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * AutoDisableConnectionActivity.
 */
@ActivityInterface
public interface AutoDisableConnectionActivity {

  /**
   * AutoDisableConnectionActivityInput.
   */
  class AutoDisableConnectionActivityInput {

    private UUID connectionId;

    @Deprecated(forRemoval = true)
    private Instant currTimestamp;

    public AutoDisableConnectionActivityInput() {}

    public UUID getConnectionId() {
      return connectionId;
    }

    public void setConnectionId(UUID connectionId) {
      this.connectionId = connectionId;
    }

    public Instant getCurrTimestamp() {
      return currTimestamp;
    }

    public void setCurrTimestamp(Instant currTimestamp) {
      this.currTimestamp = currTimestamp;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      AutoDisableConnectionActivityInput that = (AutoDisableConnectionActivityInput) o;
      return Objects.equals(connectionId, that.connectionId) && Objects.equals(currTimestamp, that.currTimestamp);
    }

    @Override
    public int hashCode() {
      return Objects.hash(connectionId, currTimestamp);
    }

    @Override
    public String toString() {
      return "AutoDisableConnectionActivityInput{connectionId=" + connectionId + ", currTimestamp=" + currTimestamp + '}';
    }

  }

  /**
   * AutoDisableConnectionOutput.
   */
  class AutoDisableConnectionOutput {

    private boolean disabled;

    public AutoDisableConnectionOutput(boolean disabled) {
      this.disabled = disabled;
    }

    public AutoDisableConnectionOutput() {}

    public boolean isDisabled() {
      return disabled;
    }

    public void setDisabled(boolean disabled) {
      this.disabled = disabled;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      AutoDisableConnectionOutput that = (AutoDisableConnectionOutput) o;
      return disabled == that.disabled;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(disabled);
    }

    @Override
    public String toString() {
      return "AutoDisableConnectionOutput{disabled=" + disabled + '}';
    }

  }

  /**
   * Disable a connection if no successful sync jobs in the last MAX_FAILURE_JOBS_IN_A_ROW job
   * attempts or the last MAX_DAYS_OF_STRAIGHT_FAILURE days (minimum 1 job attempt): disable
   * connection to prevent wasting resources.
   */
  @ActivityMethod
  AutoDisableConnectionOutput autoDisableFailingConnection(AutoDisableConnectionActivityInput input);

}
