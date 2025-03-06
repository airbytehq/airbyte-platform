/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Activity that fetches feature flags.
 */
@ActivityInterface
public interface FeatureFlagFetchActivity {

  /**
   * Feature flag fetch input.
   */
  class FeatureFlagFetchInput {

    private UUID connectionId;

    public FeatureFlagFetchInput() {}

    public FeatureFlagFetchInput(UUID connectionId) {
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
      FeatureFlagFetchInput that = (FeatureFlagFetchInput) o;
      return Objects.equals(connectionId, that.connectionId);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(connectionId);
    }

    @Override
    public String toString() {
      return "FeatureFlagFetchInput{connectionId=" + connectionId + '}';
    }

  }

  /**
   * Feature flag fetch output.
   */
  class FeatureFlagFetchOutput {

    private Map<String, Boolean> featureFlags;

    public FeatureFlagFetchOutput() {}

    public FeatureFlagFetchOutput(Map<String, Boolean> featureFlags) {
      this.featureFlags = featureFlags;
    }

    public Map<String, Boolean> getFeatureFlags() {
      return featureFlags;
    }

    public void setFeatureFlags(Map<String, Boolean> featureFlags) {
      this.featureFlags = featureFlags;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FeatureFlagFetchOutput that = (FeatureFlagFetchOutput) o;
      return Objects.equals(featureFlags, that.featureFlags);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(featureFlags);
    }

    @Override
    public String toString() {
      return "FeatureFlagFetchOutput{featureFlags=" + featureFlags + '}';
    }

  }

  /**
   * Return latest value for feature flags relevant to the ConnectionManagerWorkflow.
   */
  @ActivityMethod
  FeatureFlagFetchOutput getFeatureFlags(FeatureFlagFetchInput input);

}
