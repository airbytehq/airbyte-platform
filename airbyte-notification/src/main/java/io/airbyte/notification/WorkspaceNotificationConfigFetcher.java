/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.NotificationItem;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fetching notification settings from workspace.
 */
@Singleton
public class WorkspaceNotificationConfigFetcher {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final AirbyteApiClient airbyteApiClient;

  public WorkspaceNotificationConfigFetcher(final AirbyteApiClient airbyteApiClient) {
    this.airbyteApiClient = airbyteApiClient;
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
  public NotificationItemWithCustomerIoConfig fetchNotificationConfig(final UUID connectionId, NotificationEvent notificationEvent)
      throws IOException {
    final WorkspaceRead workspaceRead = airbyteApiClient.getWorkspaceApi().getWorkspaceByConnectionId(new ConnectionIdRequestBody(connectionId));
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
