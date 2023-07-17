/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.airbyte.commons.temporal.scheduling.retries.RetryManager;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Sends and retrieves retry state data from persistence.
 */
@ActivityInterface
public interface RetryStatePersistenceActivity {

  /**
   * Input for hydrate activity method.
   */
  @AllArgsConstructor
  @NoArgsConstructor
  @Data
  class HydrateInput {

    private Long jobId;
    private UUID connectionId;

  }

  /**
   * Output for hydrate activity method.
   */
  @AllArgsConstructor
  @NoArgsConstructor
  @Data
  class HydrateOutput {

    private RetryManager manager;

  }

  /**
   * Input for persist activity method.
   */
  @AllArgsConstructor
  @NoArgsConstructor
  @Data
  class PersistInput {

    private Long jobId;
    private UUID connectionId;
    private RetryManager manager;

  }

  /**
   * Output for persist activity method.
   */
  @AllArgsConstructor
  @NoArgsConstructor
  @Data
  class PersistOutput {

    private Boolean success;

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
