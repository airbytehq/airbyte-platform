/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync;

import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.Context;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.Organization;
import io.airbyte.featureflag.UseWorkloadApi;
import io.airbyte.featureflag.UseWorkloadOutputDocStore;
import io.airbyte.featureflag.Workspace;
import jakarta.inject.Singleton;
import java.util.ArrayList;

// TODO: remove this once migration to workloads complete
@Singleton
public class WorkloadFeatureFlagActivityImpl implements WorkloadFeatureFlagActivity {

  private final FeatureFlagClient featureFlagClient;

  public WorkloadFeatureFlagActivityImpl(final FeatureFlagClient featureFlagClient) {
    this.featureFlagClient = featureFlagClient;
  }

  @Override
  public Boolean useWorkloadApi(final WorkloadFeatureFlagActivity.Input input) {
    final var context = getContext(input);

    return featureFlagClient.boolVariation(UseWorkloadApi.INSTANCE, context);
  }

  @Override
  public Boolean useOutputDocStore(final Input input) {
    final var context = getContext(input);

    return featureFlagClient.boolVariation(UseWorkloadOutputDocStore.INSTANCE, context);
  }

  private Context getContext(final Input input) {
    final var contexts = new ArrayList<Context>();
    contexts.add(new Workspace(input.getWorkspaceId()));
    contexts.add(new Connection(input.getConnectionId()));
    final var organizationId = input.getOrganizationId();
    if (organizationId != null) {
      contexts.add(new Organization(organizationId));
    }

    return new Multi(contexts);
  }

}
