/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.airbyte.api.client.generated.WorkspaceApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import io.airbyte.featureflag.CheckInputGeneration;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Flag;
import io.airbyte.featureflag.Workspace;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 * Fetches feature flags to be used in temporal workflows.
 */
@Slf4j
@Singleton
public class FeatureFlagFetchActivityImpl implements FeatureFlagFetchActivity {

  private final WorkspaceApi workspaceApi;
  private final FeatureFlagClient featureFlagClient;

  public FeatureFlagFetchActivityImpl(final WorkspaceApi workspaceApi,
                                      final FeatureFlagClient featureFlagClient) {
    this.workspaceApi = workspaceApi;
    this.featureFlagClient = featureFlagClient;
  }

  /**
   * Get workspace id for a connection id.
   *
   * @param connectionId connection id
   * @return workspace id
   */
  public UUID getWorkspaceId(final UUID connectionId) {
    try {
      final WorkspaceRead workspace = workspaceApi.getWorkspaceByConnectionId(new ConnectionIdRequestBody().connectionId(connectionId));
      return workspace.getWorkspaceId();
    } catch (final ApiException e) {
      throw new RuntimeException("Unable to get workspace ID for connection", e);
    }
  }

  @Override
  public FeatureFlagFetchOutput getFeatureFlags(final FeatureFlagFetchInput input) {
    final UUID workspaceId = getWorkspaceId(input.getConnectionId());

    final List<Flag> workspaceFlags = List.of(CheckInputGeneration.INSTANCE);
    final Map<String, Boolean> featureFlags = new HashMap<>();
    for (final Flag flag : workspaceFlags) {
      featureFlags.put(flag.getKey(), featureFlagClient.enabled(flag, new Workspace(workspaceId)));
    }

    return new FeatureFlagFetchOutput(featureFlags);
  }

}
