/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

// TODO: remove this once migration to workloads complete
@ActivityInterface
public interface WorkloadFeatureFlagActivity {

  @AllArgsConstructor
  @NoArgsConstructor
  @Data
  class Input {

    private UUID workspaceId;
    private UUID connectionId;
    private UUID organizationId;

  }

  @ActivityMethod
  Boolean useWorkloadApi(final Input input);

  @ActivityMethod
  Boolean useOutputDocStore(final Input input);

}
