/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import java.time.Instant;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * AutoDisableConnectionActivity.
 */
@ActivityInterface
public interface AutoDisableConnectionActivity {

  /**
   * AutoDisableConnectionActivityInput.
   */
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  class AutoDisableConnectionActivityInput {

    private UUID connectionId;

    @Deprecated(forRemoval = true)
    private Instant currTimestamp;

  }

  /**
   * AutoDisableConnectionOutput.
   */
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  class AutoDisableConnectionOutput {

    private boolean disabled;

  }

  /**
   * Disable a connection if no successful sync jobs in the last MAX_FAILURE_JOBS_IN_A_ROW job
   * attempts or the last MAX_DAYS_OF_STRAIGHT_FAILURE days (minimum 1 job attempt): disable
   * connection to prevent wasting resources.
   */
  @ActivityMethod
  AutoDisableConnectionOutput autoDisableFailingConnection(AutoDisableConnectionActivityInput input);

}
