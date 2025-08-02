/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import io.airbyte.api.model.generated.NotificationItem
import io.airbyte.api.model.generated.NotificationSettings
import io.airbyte.api.model.generated.NotificationType
import io.airbyte.api.model.generated.SlackNotificationConfiguration
import io.airbyte.commons.server.converters.NotificationSettingsConverter.toApi
import io.airbyte.commons.server.converters.NotificationSettingsConverter.toClientApi
import io.airbyte.commons.server.converters.NotificationSettingsConverter.toConfig
import io.airbyte.config.Notification
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class NotificationSettingsConverterTest {
  @Test
  fun testConvertToPrototype() {
    Assertions.assertEquals(toConfig(API_NOTIFICATION_SETTINGS), PROTOCOL_NOTIFICATION_SETTINGS)
    Assertions.assertEquals(toConfig(EMPTY_API_NOTIFICATION_SETTINGS), EMPTY_CONFIG_NOTIFICATION_SETTINGS)
  }

  @Test
  fun testConvertToApi() {
    Assertions.assertEquals(toApi(PROTOCOL_NOTIFICATION_SETTINGS), API_NOTIFICATION_SETTINGS)
    val api = toApi(EMPTY_CONFIG_NOTIFICATION_SETTINGS)
    // validate we default the webhook to an empty string to satisfy the OpenAPI driven deserializers.
    Assertions.assertEquals("", api.getSendOnSuccess().getSlackConfiguration().getWebhook())
    Assertions.assertEquals("", api.getSendOnFailure().getSlackConfiguration().getWebhook())
    Assertions.assertEquals("", api.getSendOnConnectionUpdate().getSlackConfiguration().getWebhook())
    Assertions.assertEquals("", api.getSendOnSyncDisabled().getSlackConfiguration().getWebhook())
    Assertions.assertEquals("", api.getSendOnSyncDisabledWarning().getSlackConfiguration().getWebhook())
    Assertions.assertEquals("", api.getSendOnConnectionUpdateActionRequired().getSlackConfiguration().getWebhook())
  }

  @Test
  fun testConvertToApiWithNullValues() {
    val notificationSettings = io.airbyte.config.NotificationSettings()
    val api = toClientApi(notificationSettings)
    Assertions.assertEquals(
      notificationSettings.getSendOnBreakingChangeSyncsDisabled(),
      api.sendOnBreakingChangeSyncsDisabled,
    )
    Assertions.assertEquals(notificationSettings.getSendOnFailure(), api.sendOnFailure)
    Assertions.assertEquals(notificationSettings.getSendOnSuccess(), api.sendOnSuccess)
    Assertions.assertEquals(notificationSettings.getSendOnConnectionUpdate(), api.sendOnConnectionUpdate)
    Assertions.assertEquals(notificationSettings.getSendOnSyncDisabled(), api.sendOnSyncDisabled)
    Assertions.assertEquals(
      notificationSettings.getSendOnConnectionUpdateActionRequired(),
      api.sendOnConnectionUpdateActionRequired,
    )
    Assertions.assertEquals(
      notificationSettings.getSendOnConnectionUpdateActionRequired(),
      api.sendOnConnectionUpdateActionRequired,
    )
    Assertions.assertEquals(notificationSettings.getSendOnSyncDisabledWarning(), api.sendOnSyncDisabledWarning)
  }

  companion object {
    private val API_NOTIFICATION_SETTINGS: NotificationSettings? =
      NotificationSettings()
        .sendOnFailure(
          NotificationItem()
            .addNotificationTypeItem(NotificationType.CUSTOMERIO)
            .addNotificationTypeItem(NotificationType.SLACK)
            .slackConfiguration(SlackNotificationConfiguration().webhook("webhook")),
        ).sendOnConnectionUpdate(
          NotificationItem()
            .addNotificationTypeItem(NotificationType.SLACK)
            .slackConfiguration(SlackNotificationConfiguration().webhook("webhook2")),
        )

    private val PROTOCOL_NOTIFICATION_SETTINGS: io.airbyte.config.NotificationSettings? =
      io.airbyte.config
        .NotificationSettings()
        .withSendOnFailure(
          io.airbyte.config
            .NotificationItem()
            .withNotificationType(
              listOf(Notification.NotificationType.CUSTOMERIO, Notification.NotificationType.SLACK),
            ).withSlackConfiguration(
              io.airbyte.config
                .SlackNotificationConfiguration()
                .withWebhook("webhook"),
            ),
        ).withSendOnConnectionUpdate(
          io.airbyte.config
            .NotificationItem()
            .withNotificationType(listOf(Notification.NotificationType.SLACK))
            .withSlackConfiguration(
              io.airbyte.config
                .SlackNotificationConfiguration()
                .withWebhook("webhook2"),
            ),
        )

    private val EMPTY_API_NOTIFICATION_SETTINGS: NotificationSettings? =
      NotificationSettings()
        .sendOnSuccess(
          NotificationItem().notificationType(mutableListOf<NotificationType?>()).slackConfiguration(SlackNotificationConfiguration()),
        ).sendOnFailure(
          NotificationItem().notificationType(mutableListOf<NotificationType?>()).slackConfiguration(SlackNotificationConfiguration()),
        ).sendOnConnectionUpdate(
          NotificationItem().notificationType(mutableListOf<NotificationType?>()).slackConfiguration(
            SlackNotificationConfiguration(),
          ),
        ).sendOnSyncDisabled(
          NotificationItem().notificationType(mutableListOf<NotificationType?>()).slackConfiguration(SlackNotificationConfiguration()),
        ).sendOnSyncDisabledWarning(
          NotificationItem().notificationType(mutableListOf<NotificationType?>()).slackConfiguration(
            SlackNotificationConfiguration(),
          ),
        ).sendOnConnectionUpdateActionRequired(
          NotificationItem().notificationType(mutableListOf<NotificationType?>()).slackConfiguration(SlackNotificationConfiguration()),
        )

    private val EMPTY_CONFIG_NOTIFICATION_SETTINGS: io.airbyte.config.NotificationSettings? =
      io.airbyte.config
        .NotificationSettings()
        .withSendOnSuccess(
          io.airbyte.config
            .NotificationItem()
            .withNotificationType(mutableListOf<Notification.NotificationType?>())
            .withSlackConfiguration(io.airbyte.config.SlackNotificationConfiguration()),
        ).withSendOnFailure(
          io.airbyte.config
            .NotificationItem()
            .withNotificationType(mutableListOf<Notification.NotificationType?>())
            .withSlackConfiguration(io.airbyte.config.SlackNotificationConfiguration()),
        ).withSendOnConnectionUpdate(
          io.airbyte.config
            .NotificationItem()
            .withNotificationType(mutableListOf<Notification.NotificationType?>())
            .withSlackConfiguration(io.airbyte.config.SlackNotificationConfiguration()),
        ).withSendOnSyncDisabled(
          io.airbyte.config
            .NotificationItem()
            .withNotificationType(mutableListOf<Notification.NotificationType?>())
            .withSlackConfiguration(io.airbyte.config.SlackNotificationConfiguration()),
        ).withSendOnSyncDisabledWarning(
          io.airbyte.config
            .NotificationItem()
            .withNotificationType(mutableListOf<Notification.NotificationType?>())
            .withSlackConfiguration(io.airbyte.config.SlackNotificationConfiguration()),
        ).withSendOnConnectionUpdateActionRequired(
          io.airbyte.config
            .NotificationItem()
            .withNotificationType(mutableListOf<Notification.NotificationType?>())
            .withSlackConfiguration(io.airbyte.config.SlackNotificationConfiguration()),
        )
  }
}
