/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static org.mockito.Mockito.mock;

import io.airbyte.api.client.generated.WorkspaceApi;
import io.airbyte.featureflag.FeatureFlagClient;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FeatureFlagFetchActivityTest {

  private static final UUID CONNECTION_ID = UUID.randomUUID();

  FeatureFlagFetchActivity featureFlagFetchActivity;
  FeatureFlagClient featureFlagClient;

  @BeforeEach
  void setUp() {
    final WorkspaceApi workspaceApi = mock(WorkspaceApi.class);

    featureFlagFetchActivity = new FeatureFlagFetchActivityImpl(workspaceApi);
  }

  @Test
  void testGetFeatureFlags() {
    final FeatureFlagFetchActivity.FeatureFlagFetchInput input = new FeatureFlagFetchActivity.FeatureFlagFetchInput(CONNECTION_ID);

    final FeatureFlagFetchActivity.FeatureFlagFetchOutput output = featureFlagFetchActivity.getFeatureFlags(input);
    Assertions.assertEquals(output.getFeatureFlags(), Map.of());

  }

}
