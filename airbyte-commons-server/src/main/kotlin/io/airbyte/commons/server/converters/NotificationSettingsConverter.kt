/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import io.airbyte.api.model.generated.NotificationItem
import io.airbyte.api.model.generated.NotificationSettings
import io.airbyte.api.model.generated.NotificationType
import io.airbyte.api.model.generated.SlackNotificationConfiguration
import io.airbyte.commons.enums.convertTo
import io.airbyte.config.Notification
import java.util.stream.Collectors

/**
 * Convert between API and internal versions of notification models.
 */
object NotificationSettingsConverter {
  @JvmStatic
  fun toConfig(notification: NotificationSettings?): io.airbyte.config.NotificationSettings {
    val configNotificationSettings = io.airbyte.config.NotificationSettings()
    if (notification == null) {
      return configNotificationSettings
    }

    if (notification.sendOnSuccess != null) {
      configNotificationSettings.sendOnSuccess = toConfig(notification.sendOnSuccess)
    }
    if (notification.sendOnFailure != null) {
      configNotificationSettings.sendOnFailure = toConfig(notification.sendOnFailure)
    }
    if (notification.sendOnConnectionUpdate != null) {
      configNotificationSettings.sendOnConnectionUpdate = toConfig(notification.sendOnConnectionUpdate)
    }
    if (notification.sendOnSyncDisabled != null) {
      configNotificationSettings.sendOnSyncDisabled = toConfig(notification.sendOnSyncDisabled)
    }
    if (notification.sendOnSyncDisabledWarning != null) {
      configNotificationSettings.sendOnSyncDisabledWarning = toConfig(notification.sendOnSyncDisabledWarning)
    }
    if (notification.sendOnConnectionUpdateActionRequired != null) {
      configNotificationSettings.sendOnConnectionUpdateActionRequired =
        toConfig(notification.sendOnConnectionUpdateActionRequired)
    }
    if (notification.sendOnBreakingChangeWarning != null) {
      configNotificationSettings.sendOnBreakingChangeWarning = toConfig(notification.sendOnBreakingChangeWarning)
    }
    if (notification.sendOnBreakingChangeSyncsDisabled != null) {
      configNotificationSettings.sendOnBreakingChangeSyncsDisabled =
        toConfig(notification.sendOnBreakingChangeSyncsDisabled)
    }

    return configNotificationSettings
  }

  // Currently customerIoConfiguration is an empty object, so we tend to keep it as null.
  private fun toConfig(notificationItem: NotificationItem): io.airbyte.config.NotificationItem {
    val result =
      io.airbyte.config
        .NotificationItem()
        .withNotificationType(
          notificationItem.notificationType
            .stream()
            .map { notificationType: NotificationType ->
              notificationType.convertTo<Notification.NotificationType>()
            }.collect(
              Collectors.toList(),
            ),
        )

    if (notificationItem.slackConfiguration != null) {
      result.withSlackConfiguration(toConfig(notificationItem.slackConfiguration))
    }
    return result
  }

  /**
   * Convert SlackNotificationConfiguration from api to config. Used in notifications/trywebhook api
   * path.
   */
  @JvmStatic
  fun toConfig(notification: SlackNotificationConfiguration?): io.airbyte.config.SlackNotificationConfiguration {
    if (notification == null) {
      return io.airbyte.config.SlackNotificationConfiguration()
    }
    return io.airbyte.config
      .SlackNotificationConfiguration()
      .withWebhook(notification.webhook)
  }

  @JvmStatic
  fun toApi(notificationSettings: io.airbyte.config.NotificationSettings?): NotificationSettings {
    val apiNotificationSetings = NotificationSettings()
    if (notificationSettings == null) {
      return apiNotificationSetings
    }

    if (notificationSettings.sendOnSuccess != null) {
      apiNotificationSetings.sendOnSuccess = toApi(notificationSettings.sendOnSuccess)
    }
    if (notificationSettings.sendOnFailure != null) {
      apiNotificationSetings.sendOnFailure = toApi(notificationSettings.sendOnFailure)
    }
    if (notificationSettings.sendOnConnectionUpdate != null) {
      apiNotificationSetings.sendOnConnectionUpdate = toApi(notificationSettings.sendOnConnectionUpdate)
    }
    if (notificationSettings.sendOnSyncDisabled != null) {
      apiNotificationSetings.sendOnSyncDisabled = toApi(notificationSettings.sendOnSyncDisabled)
    }
    if (notificationSettings.sendOnSyncDisabledWarning != null) {
      apiNotificationSetings.sendOnSyncDisabledWarning = toApi(notificationSettings.sendOnSyncDisabledWarning)
    }
    if (notificationSettings.sendOnConnectionUpdateActionRequired != null) {
      apiNotificationSetings.sendOnConnectionUpdateActionRequired =
        toApi(notificationSettings.sendOnConnectionUpdateActionRequired)
    }
    if (notificationSettings.sendOnBreakingChangeWarning != null) {
      apiNotificationSetings.sendOnBreakingChangeWarning = toApi(notificationSettings.sendOnBreakingChangeWarning)
    }
    if (notificationSettings.sendOnBreakingChangeSyncsDisabled != null) {
      apiNotificationSetings.sendOnBreakingChangeSyncsDisabled =
        toApi(notificationSettings.sendOnBreakingChangeSyncsDisabled)
    }
    return apiNotificationSetings
  }

  private fun toApi(notificationItem: io.airbyte.config.NotificationItem): NotificationItem {
    val result =
      NotificationItem()
        .notificationType(
          notificationItem.notificationType
            .stream()
            .map { notificationType: Notification.NotificationType ->
              notificationType.convertTo<NotificationType>()
            }.collect(
              Collectors.toList(),
            ),
        )
    if (notificationItem.slackConfiguration != null) {
      result.slackConfiguration(toApi(notificationItem.slackConfiguration))
    }
    return result
  }

  private fun toApi(notification: io.airbyte.config.SlackNotificationConfiguration?): SlackNotificationConfiguration {
    // webhook is non-nullable field in the OpenAPI spec and some configurations have been saved with
    // a null webhook, so we must default to an empty string or we incur deser errors.
    if (notification == null || notification.webhook == null) {
      return SlackNotificationConfiguration()
        .webhook("")
    }
    return SlackNotificationConfiguration()
      .webhook(notification.webhook)
  }

  @JvmStatic
  fun toClientApi(notificationSettings: io.airbyte.config.NotificationSettings?): io.airbyte.api.client.model.generated.NotificationSettings {
    if (notificationSettings == null) {
      return io.airbyte.api.client.model.generated
        .NotificationSettings(null, null, null, null, null, null, null, null)
    }

    val apiClientNotificationSettings =
      io.airbyte.api.client.model.generated.NotificationSettings(
        toClientApi(notificationSettings.sendOnSuccess),
        toClientApi(notificationSettings.sendOnFailure),
        toClientApi(notificationSettings.sendOnSyncDisabled),
        toClientApi(notificationSettings.sendOnSyncDisabledWarning),
        toClientApi(notificationSettings.sendOnConnectionUpdate),
        toClientApi(notificationSettings.sendOnConnectionUpdateActionRequired),
        toClientApi(notificationSettings.sendOnBreakingChangeWarning),
        toClientApi(notificationSettings.sendOnBreakingChangeSyncsDisabled),
      )

    return apiClientNotificationSettings
  }

  private fun toClientApi(notificationItem: io.airbyte.config.NotificationItem?): io.airbyte.api.client.model.generated.NotificationItem? =
    if (notificationItem != null) convertNotificationItem(notificationItem) else null

  private fun convertNotificationItem(notificationItem: io.airbyte.config.NotificationItem): io.airbyte.api.client.model.generated.NotificationItem =
    io.airbyte.api.client.model.generated.NotificationItem(
      notificationItem.notificationType
        .stream()
        .map { n: Notification.NotificationType ->
          n.convertTo<io.airbyte.api.client.model.generated.NotificationType>()
        }.toList(),
      if (notificationItem.slackConfiguration != null) {
        io.airbyte.api.client.model.generated.SlackNotificationConfiguration(
          if (notificationItem.slackConfiguration.webhook != null) notificationItem.slackConfiguration.webhook else "",
        )
      } else {
        null
      },
      null,
    )
}
