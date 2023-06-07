/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.airbyte.api.model.generated.NotificationItem;
import io.airbyte.api.model.generated.NotificationType;
import io.airbyte.api.model.generated.SlackNotificationConfiguration;
import java.util.List;
import org.junit.jupiter.api.Test;

class NotificationSettingsConverterTest {

  private static final io.airbyte.api.model.generated.NotificationSettings API_NOTIFICATION_SETTINGS =
      new io.airbyte.api.model.generated.NotificationSettings().sendOnFailure(
          new NotificationItem()
              .addNotificationTypeItem(NotificationType.CUSTOMERIO)
              .addNotificationTypeItem(NotificationType.SLACK)
              .slackConfiguration(new SlackNotificationConfiguration().webhook("webhook")))
          .sendOnConnectionUpdate(new NotificationItem()
              .addNotificationTypeItem(NotificationType.SLACK)
              .slackConfiguration(new SlackNotificationConfiguration().webhook("webhook2")));

  private static final io.airbyte.config.NotificationSettings PROTOCOL_NOTIFICATION_SETTINGS =
      new io.airbyte.config.NotificationSettings().withSendOnFailure(
          new io.airbyte.config.NotificationItem()
              .withNotificationType(
                  List.of(io.airbyte.config.Notification.NotificationType.CUSTOMERIO, io.airbyte.config.Notification.NotificationType.SLACK))
              .withSlackConfiguration(new io.airbyte.config.SlackNotificationConfiguration().withWebhook("webhook")))
          .withSendOnConnectionUpdate(
              new io.airbyte.config.NotificationItem().withNotificationType(List.of(io.airbyte.config.Notification.NotificationType.SLACK))
                  .withSlackConfiguration(new io.airbyte.config.SlackNotificationConfiguration().withWebhook("webhook2")));

  private static final io.airbyte.api.model.generated.NotificationSettings EMPTY_API_NOTIFICATION_SETTINGS =
      new io.airbyte.api.model.generated.NotificationSettings()
          .sendOnSuccess(new NotificationItem().notificationType(List.of()).slackConfiguration(new SlackNotificationConfiguration()))
          .sendOnFailure(new NotificationItem().notificationType(List.of()).slackConfiguration(new SlackNotificationConfiguration()))
          .sendOnConnectionUpdate(new NotificationItem().notificationType(List.of()).slackConfiguration(new SlackNotificationConfiguration()))
          .sendOnSyncDisabled(new NotificationItem().notificationType(List.of()).slackConfiguration(new SlackNotificationConfiguration()))
          .sendOnSyncDisabledWarning(new NotificationItem().notificationType(List.of()).slackConfiguration(new SlackNotificationConfiguration()))
          .sendOnConnectionUpdateActionRequired(
              new NotificationItem().notificationType(List.of()).slackConfiguration(new SlackNotificationConfiguration()));

  private static final io.airbyte.config.NotificationSettings EMPTY_CONFIG_NOTIFICATION_SETTINGS =
      new io.airbyte.config.NotificationSettings()
          .withSendOnSuccess(new io.airbyte.config.NotificationItem().withNotificationType(List.of())
              .withSlackConfiguration(new io.airbyte.config.SlackNotificationConfiguration()))
          .withSendOnFailure(new io.airbyte.config.NotificationItem().withNotificationType(List.of())
              .withSlackConfiguration(new io.airbyte.config.SlackNotificationConfiguration()))
          .withSendOnConnectionUpdate(new io.airbyte.config.NotificationItem().withNotificationType(List.of())
              .withSlackConfiguration(new io.airbyte.config.SlackNotificationConfiguration()))
          .withSendOnSyncDisabled(new io.airbyte.config.NotificationItem().withNotificationType(List.of())
              .withSlackConfiguration(new io.airbyte.config.SlackNotificationConfiguration()))
          .withSendOnSyncDisabledWarning(new io.airbyte.config.NotificationItem().withNotificationType(List.of())
              .withSlackConfiguration(new io.airbyte.config.SlackNotificationConfiguration()))
          .withSendOnConnectionUpdateActionRequired(new io.airbyte.config.NotificationItem().withNotificationType(List.of())
              .withSlackConfiguration(new io.airbyte.config.SlackNotificationConfiguration()));

  @Test
  void testConvertToPrototype() {
    assertEquals(NotificationSettingsConverter.toConfig(API_NOTIFICATION_SETTINGS), PROTOCOL_NOTIFICATION_SETTINGS);
    assertEquals(NotificationSettingsConverter.toConfig(EMPTY_API_NOTIFICATION_SETTINGS), EMPTY_CONFIG_NOTIFICATION_SETTINGS);
  }

  @Test
  void testConvertToApi() {
    assertEquals(NotificationSettingsConverter.toApi(PROTOCOL_NOTIFICATION_SETTINGS), API_NOTIFICATION_SETTINGS);
    assertEquals(NotificationSettingsConverter.toApi(EMPTY_CONFIG_NOTIFICATION_SETTINGS), EMPTY_API_NOTIFICATION_SETTINGS);
  }

}
