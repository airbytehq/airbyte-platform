/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.api.client.model.generated.ConnectionStatus;
import io.airbyte.workers.temporal.activities.GetConnectionContextInput;
import io.airbyte.workers.temporal.activities.GetConnectionContextOutput;
import io.airbyte.workers.temporal.activities.GetLoadShedBackoffInput;
import io.airbyte.workers.temporal.activities.GetLoadShedBackoffOutput;
import io.airbyte.workers.temporal.activities.GetWebhookConfigInput;
import io.airbyte.workers.temporal.activities.GetWebhookConfigOutput;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * ConfigFetchActivity.
 */
@ActivityInterface
public interface ConfigFetchActivity {

  @ActivityMethod
  Optional<UUID> getSourceId(UUID connectionId);

  @ActivityMethod
  JsonNode getSourceConfig(UUID sourceId);

  @ActivityMethod
  Optional<ConnectionStatus> getStatus(UUID connectionId);

  /**
   * ScheduleRetrieverInput.
   */
  class ScheduleRetrieverInput {

    private UUID connectionId;

    public ScheduleRetrieverInput() {}

    public ScheduleRetrieverInput(UUID connectionId) {
      this.connectionId = connectionId;
    }

    public UUID getConnectionId() {
      return connectionId;
    }

    public void setConnectionId(UUID connectionId) {
      this.connectionId = connectionId;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ScheduleRetrieverInput that = (ScheduleRetrieverInput) o;
      return Objects.equals(connectionId, that.connectionId);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(connectionId);
    }

    @Override
    public String toString() {
      return "ScheduleRetrieverInput{connectionId=" + connectionId + '}';
    }

  }

  /**
   * ScheduleRetrieverOutput.
   */
  class ScheduleRetrieverOutput {

    private Duration timeToWait;

    public ScheduleRetrieverOutput() {}

    public ScheduleRetrieverOutput(Duration timeToWait) {
      this.timeToWait = timeToWait;
    }

    public Duration getTimeToWait() {
      return timeToWait;
    }

    public void setTimeToWait(Duration timeToWait) {
      this.timeToWait = timeToWait;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ScheduleRetrieverOutput that = (ScheduleRetrieverOutput) o;
      return Objects.equals(timeToWait, that.timeToWait);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(timeToWait);
    }

    @Override
    public String toString() {
      return "ScheduleRetrieverOutput{timeToWait=" + timeToWait + '}';
    }

  }

  /**
   * Return how much time to wait before running the next sync. It will query the DB to get the last
   * starting time of the latest terminal job (Failed, canceled or successful) and return the amount
   * of second the Workflow needs to await.
   */
  @ActivityMethod
  ScheduleRetrieverOutput getTimeToWait(final ScheduleRetrieverInput input);

  /**
   * Return a fully hydrated connection context (all the domain object ids relevant to the
   * connection).
   */
  @ActivityMethod
  GetConnectionContextOutput getConnectionContext(final GetConnectionContextInput input);

  /**
   * Return how much time to wait before checking load shed status. Consumer will wait in a loop until
   * this returns 0 or less.
   */
  @ActivityMethod
  GetLoadShedBackoffOutput getLoadShedBackoff(final GetLoadShedBackoffInput input);

  /**
   * GetMaxAttemptOutput.
   */
  record GetMaxAttemptOutput(int maxAttempt) {}

  /**
   * Return the maximum number of attempt allowed for a connection.
   */
  @ActivityMethod
  GetMaxAttemptOutput getMaxAttempt();

  @ActivityMethod
  Boolean isWorkspaceTombstone(UUID connectionId);

  @ActivityMethod
  GetWebhookConfigOutput getWebhookConfig(GetWebhookConfigInput input);

}
