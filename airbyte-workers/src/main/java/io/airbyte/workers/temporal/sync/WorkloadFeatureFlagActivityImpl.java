/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync;

import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.UseWorkloadApi;
import io.airbyte.featureflag.Workspace;
import jakarta.inject.Singleton;

// TODO: remove this once migration to workloads complete
@Singleton
public class WorkloadFeatureFlagActivityImpl implements WorkloadFeatureFlagActivity {

  private final FeatureFlagClient featureFlagClient;

  public WorkloadFeatureFlagActivityImpl(final FeatureFlagClient featureFlagClient) {
    this.featureFlagClient = featureFlagClient;
  }

  @Override
  public Boolean useWorkloadApi(final WorkloadFeatureFlagActivity.Input input) {
    final var context = new Workspace(input.getWorkspaceId());

    return featureFlagClient.boolVariation(UseWorkloadApi.INSTANCE, context);
  }

  @Override
  public Boolean useOutputDocStore(final Input input) {
    return true;
  }

}
