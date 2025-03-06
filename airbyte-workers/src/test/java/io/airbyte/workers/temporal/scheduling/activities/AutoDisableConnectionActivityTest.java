/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.ConnectionApi;
import io.airbyte.api.client.model.generated.InternalOperationResult;
import io.airbyte.workers.temporal.scheduling.activities.AutoDisableConnectionActivity.AutoDisableConnectionActivityInput;
import io.airbyte.workers.temporal.scheduling.activities.AutoDisableConnectionActivity.AutoDisableConnectionOutput;
import java.io.IOException;
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
  private AirbyteApiClient mAirbyteApiClient;

  @Mock
  private ConnectionApi connectionApi;

  private AutoDisableConnectionActivityInput activityInput;

  private AutoDisableConnectionActivityImpl autoDisableActivity;

  private static final UUID CONNECTION_ID = UUID.randomUUID();

  @BeforeEach
  void setUp() {
    activityInput = new AutoDisableConnectionActivityInput();
    activityInput.setConnectionId(CONNECTION_ID);

    autoDisableActivity = new AutoDisableConnectionActivityImpl(mAirbyteApiClient);
  }

  @Test
  void testConnectionAutoDisabled() throws IOException {
    when(mAirbyteApiClient.getConnectionApi()).thenReturn(connectionApi);
    when(connectionApi.autoDisableConnection(Mockito.any()))
        .thenReturn(new InternalOperationResult(true));
    final AutoDisableConnectionOutput output = autoDisableActivity.autoDisableFailingConnection(activityInput);
    assertTrue(output.isDisabled());
  }

  @Test
  void testConnectionNotAutoDisabled() throws IOException {
    when(mAirbyteApiClient.getConnectionApi()).thenReturn(connectionApi);
    when(connectionApi.autoDisableConnection(Mockito.any()))
        .thenReturn(new InternalOperationResult(false));
    final AutoDisableConnectionOutput output = autoDisableActivity.autoDisableFailingConnection(activityInput);
    assertFalse(output.isDisabled());
  }

}
