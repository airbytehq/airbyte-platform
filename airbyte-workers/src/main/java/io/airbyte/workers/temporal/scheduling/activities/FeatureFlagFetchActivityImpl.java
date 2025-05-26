/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.UseCommandCheck;
import io.airbyte.featureflag.UseSyncV2;
import io.airbyte.featureflag.Workspace;
import io.micronaut.http.HttpStatus;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.openapitools.client.infrastructure.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fetches feature flags to be used in temporal workflows.
 */
@Singleton
public class FeatureFlagFetchActivityImpl implements FeatureFlagFetchActivity {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final AirbyteApiClient airbyteApiClient;
  private final FeatureFlagClient featureFlagClient;

  public FeatureFlagFetchActivityImpl(final AirbyteApiClient airbyteApiClient, final FeatureFlagClient featureFlagClient) {
    this.airbyteApiClient = airbyteApiClient;
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
      final WorkspaceRead workspace =
          airbyteApiClient.getWorkspaceApi().getWorkspaceByConnectionId(new ConnectionIdRequestBody(connectionId));
      return workspace.getWorkspaceId();
    } catch (final ClientException e) {
      if (e.getStatusCode() == HttpStatus.NOT_FOUND.getCode()) {
        throw e;
      }
      throw new RetryableException(e);
    } catch (final IOException e) {
      throw new RuntimeException("Unable to get workspace ID for connection", e);
    }
  }

  @Override
  public FeatureFlagFetchOutput getFeatureFlags(final FeatureFlagFetchInput input) {
    final UUID workspaceId = getWorkspaceId(input.getConnectionId());

    final boolean useCommandCheck = featureFlagClient.boolVariation(UseCommandCheck.INSTANCE,
        new Multi(List.of(new Connection(input.getConnectionId()), new Workspace(workspaceId))));

    final boolean useSyncV2 = featureFlagClient.boolVariation(UseSyncV2.INSTANCE,
        new Multi(List.of(new Connection(input.getConnectionId()), new Workspace(workspaceId))));

    return new FeatureFlagFetchOutput(Map.of(UseCommandCheck.INSTANCE.getKey(), useCommandCheck,
        UseSyncV2.INSTANCE.getKey(), useSyncV2));
  }

}
