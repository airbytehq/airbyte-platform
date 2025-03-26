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
import io.airbyte.api.client.model.generated.NotificationItem;
import io.airbyte.api.client.model.generated.NotificationSettings;
import io.airbyte.api.client.model.generated.NotificationType;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import io.airbyte.notification.WorkspaceNotificationConfigFetcher.NotificationItemWithCustomerIoConfig;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WorkspaceNotificationConfigFetcherTest {

  private AirbyteApiClient airbyteApiClient;
  private WorkspaceApi workspaceApi;
  private WorkspaceNotificationConfigFetcher workspaceNotificationConfigFetcher;

  @BeforeEach
  void setup() {
    airbyteApiClient = mock(AirbyteApiClient.class);
    workspaceApi = mock(WorkspaceApi.class);
    when(airbyteApiClient.getWorkspaceApi()).thenReturn(workspaceApi);
    workspaceNotificationConfigFetcher = new WorkspaceNotificationConfigFetcher(airbyteApiClient);
  }

  @Test
  void testReturnTheRightConfig() throws IOException {
    final UUID connectionId = UUID.randomUUID();
    final String email = "em@il.com";
    final NotificationItem notificationItem = new NotificationItem(List.of(NotificationType.CUSTOMERIO), null, null);
    when(workspaceApi.getWorkspaceByConnectionId(new ConnectionIdRequestBody(connectionId)))
        .thenReturn(
            new WorkspaceRead(UUID.randomUUID(), UUID.randomUUID(), "name", "slug", true, UUID.randomUUID(), email, null, null, null, null, null,
                new NotificationSettings(null, null, null, null, null, notificationItem, null, null), null, null, null, null, null, null));

    NotificationItemWithCustomerIoConfig result =
        workspaceNotificationConfigFetcher.fetchNotificationConfig(connectionId, NotificationEvent.ON_BREAKING_CHANGE);
    assertEquals(notificationItem, result.getNotificationItem());
    assertEquals(email, result.getCustomerIoEmailConfig().getTo());
  }

}
