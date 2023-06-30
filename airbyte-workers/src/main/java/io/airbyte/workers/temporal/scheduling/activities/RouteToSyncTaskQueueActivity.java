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
 * This is an activity runs routes for all types for jobs. Currently it will support both SYNC and
 * CHECK. The naming is not perfect, but we tend to keep it here to avoid temporal versioning issue.
 */
@ActivityInterface
public interface RouteToSyncTaskQueueActivity {

  @ActivityMethod
  RouteToSyncTaskQueueOutput route(RouteToSyncTaskQueueInput input);

  @ActivityMethod
  RouteToSyncTaskQueueOutput routeToSync(RouteToSyncTaskQueueInput input);

  @ActivityMethod
  RouteToSyncTaskQueueOutput routeToCheckConnection(RouteToSyncTaskQueueInput input);

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
