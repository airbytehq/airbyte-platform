/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.generated.WorkspaceApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Test for the CustomerIoEmailConfigFetcher.
 */
class CustomerIoEmailConfigFetcherTest {

  private final WorkspaceApi workspaceApi = mock(WorkspaceApi.class);

  private final CustomerIoEmailConfigFetcher cloudCustomerIoEmailConfigFetcher = new CustomerIoEmailConfigFetcherImpl(workspaceApi);

  @Test
  void testReturnTheRightConfig() throws ApiException {
    final UUID connectionId = UUID.randomUUID();
    final String email = "em@il.com";
    when(workspaceApi.getWorkspaceByConnectionId(new ConnectionIdRequestBody().connectionId(connectionId)))
        .thenReturn(new WorkspaceRead().email(email));

    CustomerIoEmailConfig customerIoEmailConfig = cloudCustomerIoEmailConfigFetcher.fetchConfig(connectionId);
    assertEquals(email, customerIoEmailConfig.getTo());
  }

}
