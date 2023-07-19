/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.airbyte.api.client.generated.WorkspaceApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.UseNewRetries;
import io.airbyte.featureflag.Workspace;
import io.airbyte.workers.helpers.RetryStateClient;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.UUID;

/**
 * Concrete implementation of RetryStatePersistenceActivity. Delegates to non-temporal business
 * logic via RetryStatePersistence.
 */
@Singleton
public class RetryStatePersistenceActivityImpl implements RetryStatePersistenceActivity {

  final RetryStateClient client;
  final FeatureFlagClient featureFlagClient;
  final WorkspaceApi workspaceApi;

  public RetryStatePersistenceActivityImpl(final RetryStateClient client,
                                           final FeatureFlagClient featureFlagClient,
                                           final WorkspaceApi workspaceApi) {
    this.client = client;
    this.featureFlagClient = featureFlagClient;
    this.workspaceApi = workspaceApi;
  }

  @Override
  public HydrateOutput hydrateRetryState(final HydrateInput input) {
    final var workspaceId = getWorkspaceId(input.getConnectionId());

    final var enabled = featureFlagClient.boolVariation(UseNewRetries.INSTANCE, new Multi(List.of(
        new Connection(input.getConnectionId()),
        new Workspace(workspaceId))));

    if (!enabled) {
      return new HydrateOutput(null);
    }

    final var manager = client.hydrateRetryState(input.getJobId(), workspaceId);

    return new HydrateOutput(manager);
  }

  @Override
  public PersistOutput persistRetryState(final PersistInput input) {
    final var success = client.persistRetryState(input.getJobId(), input.getConnectionId(), input.getManager());

    return new PersistOutput(success);
  }

  private UUID getWorkspaceId(final UUID connectionId) {
    try {
      final WorkspaceRead workspace = workspaceApi.getWorkspaceByConnectionId(new ConnectionIdRequestBody().connectionId(connectionId));
      return workspace.getWorkspaceId();
    } catch (final ApiException e) {
      throw new RetryableException(e);
    }
  }

}
