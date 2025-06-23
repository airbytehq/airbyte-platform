/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.WorkspaceApi;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for the CustomerIoEmailConfigFetcher.
 */
class CustomerIoEmailConfigFetcherTest {

  private AirbyteApiClient airbyteApiClient;
  private WorkspaceApi workspaceApi;
  private CustomerIoEmailConfigFetcher cloudCustomerIoEmailConfigFetcher;

  @BeforeEach
  void setup() {
    airbyteApiClient = mock(AirbyteApiClient.class);
    workspaceApi = mock(WorkspaceApi.class);
    when(airbyteApiClient.getWorkspaceApi()).thenReturn(workspaceApi);
    cloudCustomerIoEmailConfigFetcher = new CustomerIoEmailConfigFetcherImpl(airbyteApiClient);
  }

  @Test
  void testReturnTheRightConfig() throws IOException {
    final UUID connectionId = UUID.randomUUID();
    final String email = "em@il.com";
    when(workspaceApi.getWorkspaceByConnectionId(new ConnectionIdRequestBody(connectionId)))
        .thenReturn(new WorkspaceRead(UUID.randomUUID(), UUID.randomUUID(), "name", "slug", true, UUID.randomUUID(), email, null, null, null, null,
            null, null, null, null, null, null, null, null));

    CustomerIoEmailConfig customerIoEmailConfig = cloudCustomerIoEmailConfigFetcher.fetchConfig(connectionId);
    assertEquals(email, customerIoEmailConfig.getTo());
  }

}
