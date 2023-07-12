/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.api.client.generated.ConnectionApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.InternalOperationResult;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.workers.temporal.scheduling.activities.AutoDisableConnectionActivity.AutoDisableConnectionActivityInput;
import io.airbyte.workers.temporal.scheduling.activities.AutoDisableConnectionActivity.AutoDisableConnectionOutput;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AutoDisableConnectionActivityTest {

  @Mock
  private FeatureFlags mFeatureFlags;
  @Mock
  private ConnectionApi connectionApi;

  private AutoDisableConnectionActivityInput activityInput;

  private AutoDisableConnectionActivityImpl autoDisableActivity;

  private static final UUID CONNECTION_ID = UUID.randomUUID();

  @BeforeEach
  void setUp() {
    activityInput = new AutoDisableConnectionActivityInput();
    activityInput.setConnectionId(CONNECTION_ID);

    Mockito.when(mFeatureFlags.autoDisablesFailingConnections()).thenReturn(true);

    autoDisableActivity = new AutoDisableConnectionActivityImpl(
        mFeatureFlags,
        connectionApi);
  }

  @Test
  void testConnectionAutoDisabled() throws ApiException {
    Mockito.when(connectionApi.autoDisableConnection(Mockito.any()))
        .thenReturn(new InternalOperationResult().succeeded(true));
    final AutoDisableConnectionOutput output = autoDisableActivity.autoDisableFailingConnection(activityInput);
    assertTrue(output.isDisabled());
  }

  @Test
  void testConnectionNotAutoDisabled() throws ApiException {
    Mockito.when(connectionApi.autoDisableConnection(Mockito.any()))
        .thenReturn(new InternalOperationResult().succeeded(false));
    final AutoDisableConnectionOutput output = autoDisableActivity.autoDisableFailingConnection(activityInput);
    assertFalse(output.isDisabled());
  }

  @Test
  void testFeatureFlagDisabled() {
    Mockito.when(mFeatureFlags.autoDisablesFailingConnections()).thenReturn(false);
    final AutoDisableConnectionOutput output = autoDisableActivity.autoDisableFailingConnection(activityInput);
    assertFalse(output.isDisabled());
  }

}
