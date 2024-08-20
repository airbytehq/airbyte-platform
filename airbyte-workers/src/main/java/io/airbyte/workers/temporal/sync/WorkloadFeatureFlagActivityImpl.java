/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync;

import io.airbyte.featureflag.Empty;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.UseWorkloadApi;
import io.airbyte.featureflag.WorkloadApiServerEnabled;
import io.airbyte.featureflag.WorkloadLauncherEnabled;
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
    var ffCheck = featureFlagClient.boolVariation(UseWorkloadApi.INSTANCE, new Workspace(input.getWorkspaceId()));
    var envCheck = featureFlagClient.boolVariation(WorkloadLauncherEnabled.INSTANCE, Empty.INSTANCE)
        && featureFlagClient.boolVariation(WorkloadApiServerEnabled.INSTANCE, Empty.INSTANCE);

    return ffCheck || envCheck;
  }

  @Override
  public Boolean useOutputDocStore(final Input input) {
    return true;
  }

}
