/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import io.airbyte.api.server.generated.models.NotificationItem
import io.airbyte.api.server.generated.models.NotificationSettings
import io.airbyte.api.server.generated.models.NotificationType
import io.airbyte.api.server.generated.models.SlackNotificationConfiguration
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

    notification.sendOnSuccess?.let { configNotificationSettings.sendOnSuccess = toConfig(it) }
    notification.sendOnFailure?.let { configNotificationSettings.sendOnFailure = toConfig(it) }
    notification.sendOnConnectionUpdate?.let { configNotificationSettings.sendOnConnectionUpdate = toConfig(it) }
    notification.sendOnSyncDisabled?.let { configNotificationSettings.sendOnSyncDisabled = toConfig(it) }
    notification.sendOnSyncDisabledWarning?.let { configNotificationSettings.sendOnSyncDisabledWarning = toConfig(it) }
    notification.sendOnConnectionUpdateActionRequired?.let { configNotificationSettings.sendOnConnectionUpdateActionRequired = toConfig(it) }
    notification.sendOnBreakingChangeWarning?.let { configNotificationSettings.sendOnBreakingChangeWarning = toConfig(it) }
    notification.sendOnBreakingChangeSyncsDisabled?.let { configNotificationSettings.sendOnBreakingChangeSyncsDisabled = toConfig(it) }
    notification.sendOnConnectionSyncQueued?.let { configNotificationSettings.sendOnConnectionSyncQueued = toConfig(it) }

    return configNotificationSettings
  }

  // Currently customerIoConfiguration is an empty object, so we tend to keep it as null.
  private fun toConfig(notificationItem: NotificationItem): io.airbyte.config.NotificationItem {
    val result =
      io.airbyte.config
        .NotificationItem()
        .withNotificationType(
          notificationItem.notificationType
            .orEmpty()
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
    if (notificationSettings == null) {
      return NotificationSettings()
    }
    return NotificationSettings(
      sendOnSuccess = notificationSettings.sendOnSuccess?.let { toApi(it) },
      sendOnFailure = notificationSettings.sendOnFailure?.let { toApi(it) },
      sendOnConnectionUpdate = notificationSettings.sendOnConnectionUpdate?.let { toApi(it) },
      sendOnSyncDisabled = notificationSettings.sendOnSyncDisabled?.let { toApi(it) },
      sendOnSyncDisabledWarning = notificationSettings.sendOnSyncDisabledWarning?.let { toApi(it) },
      sendOnConnectionUpdateActionRequired = notificationSettings.sendOnConnectionUpdateActionRequired?.let { toApi(it) },
      sendOnBreakingChangeWarning = notificationSettings.sendOnBreakingChangeWarning?.let { toApi(it) },
      sendOnBreakingChangeSyncsDisabled = notificationSettings.sendOnBreakingChangeSyncsDisabled?.let { toApi(it) },
      sendOnConnectionSyncQueued = notificationSettings.sendOnConnectionSyncQueued?.let { toApi(it) },
    )
  }

  private fun toApi(notificationItem: io.airbyte.config.NotificationItem): NotificationItem {
    val result =
      NotificationItem(
        notificationType =
          notificationItem.notificationType
            .stream()
            .map { notificationType: Notification.NotificationType ->
              notificationType.convertTo<NotificationType>()
            }.collect(
              Collectors.toList(),
            ),
        slackConfiguration = notificationItem.slackConfiguration?.let { toApi(it) },
      )
    return result
  }

  private fun toApi(notification: io.airbyte.config.SlackNotificationConfiguration?): SlackNotificationConfiguration {
    // webhook is non-nullable field in the OpenAPI spec and some configurations have been saved with
    // a null webhook, so we must default to an empty string or we incur deser errors.
    if (notification == null || notification.webhook == null) {
      return SlackNotificationConfiguration(webhook = "")
    }
    return SlackNotificationConfiguration(webhook = notification.webhook)
  }

  @JvmStatic
  fun toClientApi(notificationSettings: io.airbyte.config.NotificationSettings?): io.airbyte.api.client.model.generated.NotificationSettings {
    if (notificationSettings == null) {
      return io.airbyte.api.client.model.generated
        .NotificationSettings(null, null, null, null, null, null, null, null, null)
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
        toClientApi(notificationSettings.sendOnConnectionSyncQueued),
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
