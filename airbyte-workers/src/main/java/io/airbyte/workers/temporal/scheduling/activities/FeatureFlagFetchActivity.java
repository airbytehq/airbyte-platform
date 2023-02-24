/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import java.util.Map;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Activity that fetches feature flags.
 */
@ActivityInterface
public interface FeatureFlagFetchActivity {

  /**
   * Feature flag fetch input.
   */
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  class FeatureFlagFetchInput {

    private UUID connectionId;

  }

  /**
   * Feature flag fetch output.
   */
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  class FeatureFlagFetchOutput {

    private Map<String, Boolean> featureFlags;

  }

  /**
   * Return latest value for feature flags relevant to the ConnectionManagerWorkflow.
   */
  @ActivityMethod
  FeatureFlagFetchOutput getFeatureFlags(FeatureFlagFetchInput input);

}
