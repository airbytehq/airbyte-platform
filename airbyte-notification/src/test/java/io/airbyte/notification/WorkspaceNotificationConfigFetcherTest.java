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
import io.airbyte.api.client.model.generated.NotificationItem;
import io.airbyte.api.client.model.generated.NotificationSettings;
import io.airbyte.api.client.model.generated.NotificationType;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import io.airbyte.notification.WorkspaceNotificationConfigFetcher.NotificationItemWithCustomerIoConfig;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class WorkspaceNotificationConfigFetcherTest {

  private final WorkspaceApi workspaceApi = mock(WorkspaceApi.class);

  private final WorkspaceNotificationConfigFetcher workspaceNotificationConfigFetcher = new WorkspaceNotificationConfigFetcher(workspaceApi);

  @Test
  void testReturnTheRightConfig() throws ApiException {
    final UUID connectionId = UUID.randomUUID();
    final String email = "em@il.com";
    final NotificationItem notificationItem = new NotificationItem().addNotificationTypeItem(NotificationType.CUSTOMERIO);
    when(workspaceApi.getWorkspaceByConnectionId(new ConnectionIdRequestBody().connectionId(connectionId)))
        .thenReturn(
            new WorkspaceRead().email(email).notificationSettings(new NotificationSettings().sendOnConnectionUpdateActionRequired(notificationItem)));

    NotificationItemWithCustomerIoConfig result =
        workspaceNotificationConfigFetcher.fetchNotificationConfig(connectionId, NotificationEvent.ON_BREAKING_CHANGE);
    assertEquals(notificationItem, result.getNotificationItem());
    assertEquals(email, result.getCustomerIoEmailConfig().getTo());
  }

}
