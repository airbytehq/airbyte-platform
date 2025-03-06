/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.WorkspaceApi;
import io.airbyte.featureflag.TestClient;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FeatureFlagFetchActivityTest {

  private static final UUID CONNECTION_ID = UUID.randomUUID();

  private AirbyteApiClient mAirbyteApiClient;
  private WorkspaceApi mWorkspaceApi;
  private FeatureFlagFetchActivity featureFlagFetchActivity;

  @BeforeEach
  void setUp() {
    mWorkspaceApi = mock(WorkspaceApi.class);
    mAirbyteApiClient = mock(AirbyteApiClient.class);
    when(mAirbyteApiClient.getWorkspaceApi()).thenReturn(mWorkspaceApi);
    featureFlagFetchActivity = new FeatureFlagFetchActivityImpl(mAirbyteApiClient, new TestClient());
  }

  @Test
  void testGetFeatureFlags() {
    final FeatureFlagFetchActivity.FeatureFlagFetchInput input = new FeatureFlagFetchActivity.FeatureFlagFetchInput(CONNECTION_ID);

    assertDoesNotThrow(() -> featureFlagFetchActivity.getFeatureFlags(input));
  }

}
