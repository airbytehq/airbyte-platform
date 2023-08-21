/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.airbyte.api.client.generated.WorkspaceApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 * Fetches feature flags to be used in temporal workflows.
 */
@Slf4j
@Singleton
public class FeatureFlagFetchActivityImpl implements FeatureFlagFetchActivity {

  private final WorkspaceApi workspaceApi;

  public FeatureFlagFetchActivityImpl(final WorkspaceApi workspaceApi) {
    this.workspaceApi = workspaceApi;
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
    // No feature flags are currently in use.
    // To get value for a feature flag with the workspace context, add it to the workspaceFlags list.
    return new FeatureFlagFetchOutput(new HashMap<>());
  }

}
