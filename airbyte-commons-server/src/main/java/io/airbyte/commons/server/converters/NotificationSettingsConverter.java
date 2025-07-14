/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters;

import io.airbyte.commons.enums.Enums;
import java.util.stream.Collectors;

/**
 * Convert between API and internal versions of notification models.
 */
@SuppressWarnings("LineLength")
public class NotificationSettingsConverter {

  public static io.airbyte.config.NotificationSettings toConfig(final io.airbyte.api.model.generated.NotificationSettings notification) {
    final io.airbyte.config.NotificationSettings configNotificationSettings = new io.airbyte.config.NotificationSettings();
    if (notification == null) {
      return configNotificationSettings;
    }

    if (notification.getSendOnSuccess() != null) {
      configNotificationSettings.setSendOnSuccess(toConfig(notification.getSendOnSuccess()));
    }
    if (notification.getSendOnFailure() != null) {
      configNotificationSettings.setSendOnFailure(toConfig(notification.getSendOnFailure()));
    }
    if (notification.getSendOnConnectionUpdate() != null) {
      configNotificationSettings.setSendOnConnectionUpdate(toConfig(notification.getSendOnConnectionUpdate()));
    }
    if (notification.getSendOnSyncDisabled() != null) {
      configNotificationSettings.setSendOnSyncDisabled(toConfig(notification.getSendOnSyncDisabled()));
    }
    if (notification.getSendOnSyncDisabledWarning() != null) {
      configNotificationSettings.setSendOnSyncDisabledWarning(toConfig(notification.getSendOnSyncDisabledWarning()));
    }
    if (notification.getSendOnConnectionUpdateActionRequired() != null) {
      configNotificationSettings.setSendOnConnectionUpdateActionRequired(toConfig(notification.getSendOnConnectionUpdateActionRequired()));
    }
    if (notification.getSendOnBreakingChangeWarning() != null) {
      configNotificationSettings.setSendOnBreakingChangeWarning(toConfig(notification.getSendOnBreakingChangeWarning()));
    }
    if (notification.getSendOnBreakingChangeSyncsDisabled() != null) {
      configNotificationSettings.setSendOnBreakingChangeSyncsDisabled(toConfig(notification.getSendOnBreakingChangeSyncsDisabled()));
    }

    return configNotificationSettings;
  }

  // Currently customerIoConfiguration is an empty object, so we tend to keep it as null.
  private static io.airbyte.config.NotificationItem toConfig(final io.airbyte.api.model.generated.NotificationItem notificationItem) {
    final io.airbyte.config.NotificationItem result = new io.airbyte.config.NotificationItem()
        .withNotificationType(notificationItem.getNotificationType().stream()
            .map(notificationType -> Enums.convertTo(notificationType, io.airbyte.config.Notification.NotificationType.class)).collect(
                Collectors.toList()));

    if (notificationItem.getSlackConfiguration() != null) {
      result.withSlackConfiguration(toConfig(notificationItem.getSlackConfiguration()));
    }
    return result;
  }

  /**
   * Convert SlackNotificationConfiguration from api to config. Used in notifications/trywebhook api
   * path.
   */
  public static io.airbyte.config.SlackNotificationConfiguration toConfig(final io.airbyte.api.model.generated.SlackNotificationConfiguration notification) {
    if (notification == null) {
      return new io.airbyte.config.SlackNotificationConfiguration();
    }
    return new io.airbyte.config.SlackNotificationConfiguration()
        .withWebhook(notification.getWebhook());
  }

  public static io.airbyte.api.model.generated.NotificationSettings toApi(final io.airbyte.config.NotificationSettings notificationSettings) {
    final io.airbyte.api.model.generated.NotificationSettings apiNotificationSetings = new io.airbyte.api.model.generated.NotificationSettings();
    if (notificationSettings == null) {
      return apiNotificationSetings;
    }

    if (notificationSettings.getSendOnSuccess() != null) {
      apiNotificationSetings.setSendOnSuccess(toApi(notificationSettings.getSendOnSuccess()));
    }
    if (notificationSettings.getSendOnFailure() != null) {
      apiNotificationSetings.setSendOnFailure(toApi(notificationSettings.getSendOnFailure()));
    }
    if (notificationSettings.getSendOnConnectionUpdate() != null) {
      apiNotificationSetings.setSendOnConnectionUpdate(toApi(notificationSettings.getSendOnConnectionUpdate()));
    }
    if (notificationSettings.getSendOnSyncDisabled() != null) {
      apiNotificationSetings.setSendOnSyncDisabled(toApi(notificationSettings.getSendOnSyncDisabled()));
    }
    if (notificationSettings.getSendOnSyncDisabledWarning() != null) {
      apiNotificationSetings.setSendOnSyncDisabledWarning(toApi(notificationSettings.getSendOnSyncDisabledWarning()));
    }
    if (notificationSettings.getSendOnConnectionUpdateActionRequired() != null) {
      apiNotificationSetings.setSendOnConnectionUpdateActionRequired(toApi(notificationSettings.getSendOnConnectionUpdateActionRequired()));
    }
    if (notificationSettings.getSendOnBreakingChangeWarning() != null) {
      apiNotificationSetings.setSendOnBreakingChangeWarning(toApi(notificationSettings.getSendOnBreakingChangeWarning()));
    }
    if (notificationSettings.getSendOnBreakingChangeSyncsDisabled() != null) {
      apiNotificationSetings.setSendOnBreakingChangeSyncsDisabled(toApi(notificationSettings.getSendOnBreakingChangeSyncsDisabled()));
    }
    return apiNotificationSetings;
  }

  private static io.airbyte.api.model.generated.NotificationItem toApi(final io.airbyte.config.NotificationItem notificationItem) {
    final var result = new io.airbyte.api.model.generated.NotificationItem()
        .notificationType(notificationItem.getNotificationType().stream()
            .map(notificationType -> Enums.convertTo(notificationType, io.airbyte.api.model.generated.NotificationType.class)).collect(
                Collectors.toList()));
    if (notificationItem.getSlackConfiguration() != null) {
      result.slackConfiguration(toApi(notificationItem.getSlackConfiguration()));
    }
    return result;
  }

  private static io.airbyte.api.model.generated.SlackNotificationConfiguration toApi(final io.airbyte.config.SlackNotificationConfiguration notification) {
    // webhook is non-nullable field in the OpenAPI spec and some configurations have been saved with
    // a null webhook, so we must default to an empty string or we incur deser errors.
    if (notification == null || notification.getWebhook() == null) {
      return new io.airbyte.api.model.generated.SlackNotificationConfiguration()
          .webhook("");
    }
    return new io.airbyte.api.model.generated.SlackNotificationConfiguration()
        .webhook(notification.getWebhook());
  }

  public static io.airbyte.api.client.model.generated.NotificationSettings toClientApi(final io.airbyte.config.NotificationSettings notificationSettings) {
    if (notificationSettings == null) {
      return new io.airbyte.api.client.model.generated.NotificationSettings(null, null, null, null, null, null, null, null);
    }

    final io.airbyte.api.client.model.generated.NotificationSettings apiClientNotificationSettings =
        new io.airbyte.api.client.model.generated.NotificationSettings(
            toClientApi(notificationSettings.getSendOnSuccess()),
            toClientApi(notificationSettings.getSendOnFailure()),
            toClientApi(notificationSettings.getSendOnSyncDisabled()),
            toClientApi(notificationSettings.getSendOnSyncDisabledWarning()),
            toClientApi(notificationSettings.getSendOnConnectionUpdate()),
            toClientApi(notificationSettings.getSendOnConnectionUpdateActionRequired()),
            toClientApi(notificationSettings.getSendOnBreakingChangeWarning()),
            toClientApi(notificationSettings.getSendOnBreakingChangeSyncsDisabled()));

    return apiClientNotificationSettings;
  }

  private static io.airbyte.api.client.model.generated.NotificationItem toClientApi(final io.airbyte.config.NotificationItem notificationItem) {
    return notificationItem != null ? convertNotificationItem(notificationItem) : null;
  }

  private static io.airbyte.api.client.model.generated.NotificationItem convertNotificationItem(final io.airbyte.config.NotificationItem notificationItem) {
    return new io.airbyte.api.client.model.generated.NotificationItem(
        notificationItem.getNotificationType().stream().map(n -> Enums.convertTo(n, io.airbyte.api.client.model.generated.NotificationType.class))
            .toList(),
        notificationItem.getSlackConfiguration() != null
            ? new io.airbyte.api.client.model.generated.SlackNotificationConfiguration(
                notificationItem.getSlackConfiguration().getWebhook() != null ? notificationItem.getSlackConfiguration().getWebhook() : "")
            : null,
        null);
  }

}
