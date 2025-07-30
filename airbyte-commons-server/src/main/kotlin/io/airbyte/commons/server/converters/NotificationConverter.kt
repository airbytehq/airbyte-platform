/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import io.airbyte.api.model.generated.Notification
import io.airbyte.api.model.generated.NotificationType
import io.airbyte.api.model.generated.SlackNotificationConfiguration
import io.airbyte.commons.enums.convertTo
import java.util.stream.Collectors

/**
 * Convert between API and internal versions of notification models.
 */
object NotificationConverter {
  @JvmStatic
  fun toConfigList(notifications: List<Notification>?): List<io.airbyte.config.Notification> {
    if (notifications == null) {
      return emptyList()
    }
    return notifications.stream().map { obj: Notification -> toConfig(obj) }.collect(Collectors.toList())
  }

  fun toConfig(notification: Notification): io.airbyte.config.Notification =
    io.airbyte.config
      .Notification()
      .withNotificationType(
        notification.notificationType?.convertTo<io.airbyte.config.Notification.NotificationType>(),
      ).withSendOnSuccess(notification.sendOnSuccess)
      .withSendOnFailure(notification.sendOnFailure)
      .withSlackConfiguration(toConfig(notification.slackConfiguration))

  private fun toConfig(notification: SlackNotificationConfiguration): io.airbyte.config.SlackNotificationConfiguration =
    io.airbyte.config
      .SlackNotificationConfiguration()
      .withWebhook(notification.webhook)

  @JvmStatic
  fun toApiList(notifications: List<io.airbyte.config.Notification>): List<Notification> =
    notifications
      .stream()
      .map<Notification> { obj: io.airbyte.config.Notification ->
        toApi(obj)
      }.collect(Collectors.toList())

  fun toApi(notification: io.airbyte.config.Notification): Notification =
    Notification()
      .notificationType(notification.notificationType?.convertTo<NotificationType>())
      .sendOnSuccess(notification.sendOnSuccess)
      .sendOnFailure(notification.sendOnFailure)
      .slackConfiguration(toApi(notification.slackConfiguration))

  private fun toApi(notification: io.airbyte.config.SlackNotificationConfiguration): SlackNotificationConfiguration =
    SlackNotificationConfiguration()
      .webhook(notification.webhook)

  fun toClientApiList(configNotifications: List<io.airbyte.config.Notification>?): List<io.airbyte.api.client.model.generated.Notification> {
    if (configNotifications == null) {
      return emptyList()
    }
    return configNotifications.stream().map { obj: io.airbyte.config.Notification -> toClientApi(obj) }.collect(Collectors.toList())
  }

  fun toClientApi(configNotification: io.airbyte.config.Notification): io.airbyte.api.client.model.generated.Notification {
    val slackNotificationConfiguration =
      io.airbyte.api.client.model.generated.SlackNotificationConfiguration(
        if (configNotification.slackConfiguration.webhook != null) configNotification.slackConfiguration.webhook else "",
      )
    return io.airbyte.api.client.model.generated.Notification(
      configNotification.notificationType?.convertTo<io.airbyte.api.client.model.generated.NotificationType>()!!,
      configNotification.sendOnSuccess,
      configNotification.sendOnFailure,
      slackNotificationConfiguration,
      null,
    )
  }
}
