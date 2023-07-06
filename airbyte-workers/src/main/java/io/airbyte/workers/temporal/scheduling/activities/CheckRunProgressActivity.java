/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * Activity to check whether a given run (attempt for now) made progress. Output to be used as input
 * to retry logic.
 */
@ActivityInterface
public interface CheckRunProgressActivity {

  /**
   * Input object for CheckRunProgressActivity#checkProgress.
   */
  @AllArgsConstructor
  @NoArgsConstructor
  @Data
  class Input {

    private Long jobId;
    private Integer attemptNo;
    private UUID connectionId;

  }

  /**
   * Output object for CheckRunProgressActivity#checkProgress.
   */
  @AllArgsConstructor
  @NoArgsConstructor
  @Data
  class Output {

    @Accessors(fluent = true)
    private Boolean madeProgress;

  }

  @ActivityMethod
  Output checkProgress(final Input input);

}
