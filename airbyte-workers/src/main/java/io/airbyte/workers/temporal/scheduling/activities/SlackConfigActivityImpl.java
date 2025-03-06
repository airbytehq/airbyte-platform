/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.Notification;
import io.airbyte.api.client.model.generated.NotificationType;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import io.airbyte.config.SlackNotificationConfiguration;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

/**
 * SlackConfigActivityImpl.
 */
@Singleton
public class SlackConfigActivityImpl implements SlackConfigActivity {

  private final AirbyteApiClient airbyteApiClient;

  public SlackConfigActivityImpl(final AirbyteApiClient airbyteApiClient) {
    this.airbyteApiClient = airbyteApiClient;
  }

  @Override
  public Optional<SlackNotificationConfiguration> fetchSlackConfiguration(UUID connectionId) throws IOException {
    final io.airbyte.api.client.model.generated.ConnectionIdRequestBody requestBody =
        new io.airbyte.api.client.model.generated.ConnectionIdRequestBody(connectionId);
    final WorkspaceRead workspaceRead = airbyteApiClient.getWorkspaceApi().getWorkspaceByConnectionId(requestBody);
    for (Notification notification : workspaceRead.getNotifications()) {
      if (notification.getNotificationType() == NotificationType.SLACK) {
        return Optional.of(new SlackNotificationConfiguration().withWebhook(notification.getSlackConfiguration().getWebhook()));
      }
    }
    return Optional.empty();
  }

}
