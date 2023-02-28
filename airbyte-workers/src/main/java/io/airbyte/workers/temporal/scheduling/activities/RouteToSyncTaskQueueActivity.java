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

/**
 * RouteToSyncTaskQueueActivity.
 */
@ActivityInterface
public interface RouteToSyncTaskQueueActivity {

  @ActivityMethod
  RouteToSyncTaskQueueOutput route(RouteToSyncTaskQueueInput input);

  /**
   * RouteToSyncTaskQueueInput.
   */
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  class RouteToSyncTaskQueueInput {

    private UUID connectionId;

  }

  /**
   * RouteToSyncTaskQueueOutput.
   */
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  class RouteToSyncTaskQueueOutput {

    private String taskQueue;

  }

}
