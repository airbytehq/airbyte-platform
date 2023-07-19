/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.WorkspaceApi;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.NotificationItem;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import jakarta.inject.Singleton;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 * Fetching notification settings from workspace.
 */
@Singleton
@Slf4j
public class WorkspaceNotificationConfigFetcher {

  private final WorkspaceApi workspaceApi;

  public WorkspaceNotificationConfigFetcher(WorkspaceApi workspaceApi) {
    this.workspaceApi = workspaceApi;
  }

  class NotificationItemWithCustomerIoConfig {

    NotificationItemWithCustomerIoConfig(NotificationItem notificationItem, CustomerIoEmailConfig customerIoEmailConfig) {
      this.notificationItem = notificationItem;
      this.customerIoEmailConfig = customerIoEmailConfig;
    }

    NotificationItem notificationItem;
    CustomerIoEmailConfig customerIoEmailConfig;

    NotificationItem getNotificationItem() {
      return notificationItem;
    }

    CustomerIoEmailConfig getCustomerIoEmailConfig() {
      return customerIoEmailConfig;
    }

  }

  /**
   * Fetch corresponding notificationItem based on notification action.
   */
  public NotificationItemWithCustomerIoConfig fetchNotificationConfig(final UUID connectionId, NotificationEvent notificationEvent) {
    final WorkspaceRead workspaceRead = AirbyteApiClient.retryWithJitter(
        () -> workspaceApi.getWorkspaceByConnectionId(new ConnectionIdRequestBody().connectionId(connectionId)),
        "retrieve workspace for notification use.",
        /* jitterMaxIntervalSecs= */10,
        /* finalInternvalSecs= */10,
        /* maxTries= */ 3);
    if (workspaceRead == null) {
      log.error(
          String.format("Unable to fetch workspace by connection %s. Not blocking but we are not sending any notifications. \n", connectionId));
      return new NotificationItemWithCustomerIoConfig(new NotificationItem(), new CustomerIoEmailConfig(""));
    }

    NotificationItem item;

    switch (notificationEvent) {
      case ON_BREAKING_CHANGE -> {
        item = workspaceRead.getNotificationSettings().getSendOnConnectionUpdateActionRequired();
        break;
      }
      case ON_NON_BREAKING_CHANGE -> {
        item = workspaceRead.getNotificationSettings().getSendOnConnectionUpdate();
        break;
      }
      default -> throw new RuntimeException("Unexpected notification action: " + notificationEvent);
    }

    return new NotificationItemWithCustomerIoConfig(item, new CustomerIoEmailConfig(workspaceRead.getEmail()));

  }

}
