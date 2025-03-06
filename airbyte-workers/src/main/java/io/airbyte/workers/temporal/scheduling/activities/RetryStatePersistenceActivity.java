/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.airbyte.commons.temporal.scheduling.retries.RetryManager;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import java.util.Objects;
import java.util.UUID;

/**
 * Sends and retrieves retry state data from persistence.
 */
@ActivityInterface
public interface RetryStatePersistenceActivity {

  /**
   * Input for hydrate activity method.
   */
  class HydrateInput {

    private Long jobId;
    private UUID connectionId;

    public HydrateInput() {}

    public HydrateInput(Long jobId, UUID connectionId) {
      this.jobId = jobId;
      this.connectionId = connectionId;
    }

    public Long getJobId() {
      return jobId;
    }

    public void setJobId(Long jobId) {
      this.jobId = jobId;
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
      HydrateInput that = (HydrateInput) o;
      return Objects.equals(jobId, that.jobId) && Objects.equals(connectionId, that.connectionId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(jobId, connectionId);
    }

    @Override
    public String toString() {
      return "HydrateInput{jobId=" + jobId + ", connectionId=" + connectionId + '}';
    }

  }

  /**
   * Output for hydrate activity method.
   */
  class HydrateOutput {

    private RetryManager manager;

    public HydrateOutput() {}

    public HydrateOutput(RetryManager manager) {
      this.manager = manager;
    }

    public RetryManager getManager() {
      return manager;
    }

    public void setManager(RetryManager manager) {
      this.manager = manager;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      HydrateOutput that = (HydrateOutput) o;
      return Objects.equals(manager, that.manager);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(manager);
    }

    @Override
    public String toString() {
      return "HydrateOutput{manager=" + manager + '}';
    }

  }

  /**
   * Input for persist activity method.
   */
  class PersistInput {

    private Long jobId;
    private UUID connectionId;
    private RetryManager manager;

    public PersistInput() {}

    public PersistInput(Long jobId, UUID connectionId, RetryManager manager) {
      this.jobId = jobId;
      this.connectionId = connectionId;
      this.manager = manager;
    }

    public Long getJobId() {
      return jobId;
    }

    public void setJobId(Long jobId) {
      this.jobId = jobId;
    }

    public UUID getConnectionId() {
      return connectionId;
    }

    public void setConnectionId(UUID connectionId) {
      this.connectionId = connectionId;
    }

    public RetryManager getManager() {
      return manager;
    }

    public void setManager(RetryManager manager) {
      this.manager = manager;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PersistInput that = (PersistInput) o;
      return Objects.equals(jobId, that.jobId) && Objects.equals(connectionId, that.connectionId) && Objects.equals(manager,
          that.manager);
    }

    @Override
    public int hashCode() {
      return Objects.hash(jobId, connectionId, manager);
    }

    @Override
    public String toString() {
      return "PersistInput{jobId=" + jobId + ", connectionId=" + connectionId + ", manager=" + manager + '}';
    }

  }

  /**
   * Output for persist activity method.
   */
  class PersistOutput {

    private Boolean success;

    public PersistOutput() {}

    public PersistOutput(Boolean success) {
      this.success = success;
    }

    public Boolean getSuccess() {
      return success;
    }

    public void setSuccess(Boolean success) {
      this.success = success;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PersistOutput that = (PersistOutput) o;
      return Objects.equals(success, that.success);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(success);
    }

    @Override
    public String toString() {
      return "PersistOutput{success=" + success + '}';
    }

  }

  /**
   * Hydrates a RetryStateManager with data from persistence.
   *
   * @param input jobId — the id of the current job.
   * @return HydrateOutput wih hydrated RetryStateManager or new RetryStateManager if no state exists.
   */
  @ActivityMethod
  HydrateOutput hydrateRetryState(final HydrateInput input);

  /**
   * Persist the state of a RetryStateManager.
   *
   * @param input jobId, connectionId and RetryManager to be persisted.
   * @return PersistOutput with boolean denoting success.
   */
  @ActivityMethod
  PersistOutput persistRetryState(final PersistInput input);

}
