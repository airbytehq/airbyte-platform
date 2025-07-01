/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.WorkspaceApi;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import io.airbyte.featureflag.TestClient;
import io.airbyte.featureflag.UseCommandCheck;
import io.airbyte.featureflag.UseSyncV2;
import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FeatureFlagFetchActivityTest {

  private static final UUID CONNECTION_ID = UUID.randomUUID();

  private AirbyteApiClient mAirbyteApiClient;
  private WorkspaceApi mWorkspaceApi;
  private TestClient mTestClient;
  private FeatureFlagFetchActivity featureFlagFetchActivity;

  @BeforeEach
  void setUp() throws IOException {
    mWorkspaceApi = mock(WorkspaceApi.class);
    mAirbyteApiClient = mock(AirbyteApiClient.class);
    when(mAirbyteApiClient.getWorkspaceApi()).thenReturn(mWorkspaceApi);
    when(mWorkspaceApi.getWorkspaceByConnectionId(any())).thenReturn(new WorkspaceRead(
        UUID.randomUUID(),
        UUID.randomUUID(),
        "",
        "",
        false,
        UUID.randomUUID(),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null));
    mTestClient = mock(TestClient.class);
    featureFlagFetchActivity = new FeatureFlagFetchActivityImpl(mAirbyteApiClient, mTestClient);
  }

  @Test
  void testGetFeatureFlags() {
    when(mTestClient.boolVariation(eq(UseSyncV2.INSTANCE), any())).thenReturn(true);
    when(mTestClient.boolVariation(eq(UseCommandCheck.INSTANCE), any())).thenReturn(true);

    final FeatureFlagFetchActivity.FeatureFlagFetchInput input = new FeatureFlagFetchActivity.FeatureFlagFetchInput(CONNECTION_ID);

    FeatureFlagFetchActivity.FeatureFlagFetchOutput featureFlagFetchOutput =
        featureFlagFetchActivity.getFeatureFlags(input);

    assertTrue(featureFlagFetchOutput.getFeatureFlags().get(UseSyncV2.INSTANCE.getKey()));
    assertTrue(featureFlagFetchOutput.getFeatureFlags().get(UseCommandCheck.INSTANCE.getKey()));
  }

}
