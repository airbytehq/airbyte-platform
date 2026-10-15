/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import io.airbyte.api.server.generated.models.NotificationItem
import io.airbyte.api.server.generated.models.NotificationSettings
import io.airbyte.api.server.generated.models.NotificationType
import io.airbyte.api.server.generated.models.SlackNotificationConfiguration
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
    Assertions.assertEquals(toConfig(EMPTY_API_NOTIFICATION_SETTINGS), EMPTY_WEBHOOK_CONFIG_NOTIFICATION_SETTINGS)
  }

  @Test
  fun testConvertToApi() {
    Assertions.assertEquals(toApi(PROTOCOL_NOTIFICATION_SETTINGS), API_NOTIFICATION_SETTINGS)
    val api = toApi(EMPTY_CONFIG_NOTIFICATION_SETTINGS)
    // validate we default the webhook to an empty string to satisfy the OpenAPI driven deserializers.
    Assertions.assertEquals("", api.sendOnSuccess!!.slackConfiguration!!.webhook)
    Assertions.assertEquals("", api.sendOnFailure!!.slackConfiguration!!.webhook)
    Assertions.assertEquals("", api.sendOnConnectionUpdate!!.slackConfiguration!!.webhook)
    Assertions.assertEquals("", api.sendOnSyncDisabled!!.slackConfiguration!!.webhook)
    Assertions.assertEquals("", api.sendOnSyncDisabledWarning!!.slackConfiguration!!.webhook)
    Assertions.assertEquals("", api.sendOnConnectionUpdateActionRequired!!.slackConfiguration!!.webhook)
    Assertions.assertEquals("", api.sendOnConnectionSyncQueued!!.slackConfiguration!!.webhook)
  }

  @Test
  fun testConvertToApiWithNullValues() {
    val notificationSettings = io.airbyte.config.NotificationSettings()
    val api = toClientApi(notificationSettings)
    Assertions.assertEquals(
      notificationSettings.sendOnBreakingChangeSyncsDisabled,
      api.sendOnBreakingChangeSyncsDisabled,
    )
    Assertions.assertEquals(notificationSettings.sendOnFailure, api.sendOnFailure)
    Assertions.assertEquals(notificationSettings.sendOnSuccess, api.sendOnSuccess)
    Assertions.assertEquals(notificationSettings.sendOnConnectionUpdate, api.sendOnConnectionUpdate)
    Assertions.assertEquals(notificationSettings.sendOnSyncDisabled, api.sendOnSyncDisabled)
    Assertions.assertEquals(
      notificationSettings.sendOnConnectionUpdateActionRequired,
      api.sendOnConnectionUpdateActionRequired,
    )
    Assertions.assertEquals(
      notificationSettings.sendOnConnectionUpdateActionRequired,
      api.sendOnConnectionUpdateActionRequired,
    )
    Assertions.assertEquals(notificationSettings.sendOnSyncDisabledWarning, api.sendOnSyncDisabledWarning)
    Assertions.assertEquals(notificationSettings.sendOnConnectionSyncQueued, api.sendOnConnectionSyncQueued)
  }

  companion object {
    private val API_NOTIFICATION_SETTINGS: NotificationSettings? =
      NotificationSettings(
        sendOnFailure =
          NotificationItem(
            notificationType = listOf(NotificationType.CUSTOMERIO, NotificationType.SLACK),
            slackConfiguration = SlackNotificationConfiguration(webhook = "webhook"),
          ),
        sendOnConnectionUpdate =
          NotificationItem(
            notificationType = listOf(NotificationType.SLACK),
            slackConfiguration = SlackNotificationConfiguration(webhook = "webhook2"),
          ),
        sendOnConnectionSyncQueued = NotificationItem(notificationType = listOf(NotificationType.CUSTOMERIO)),
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
        ).withSendOnConnectionSyncQueued(
          io.airbyte.config
            .NotificationItem()
            .withNotificationType(listOf(Notification.NotificationType.CUSTOMERIO)),
        )

    private val EMPTY_API_NOTIFICATION_SETTINGS: NotificationSettings? =
      NotificationSettings(
        sendOnSuccess = NotificationItem(notificationType = emptyList(), slackConfiguration = SlackNotificationConfiguration(webhook = "")),
        sendOnFailure = NotificationItem(notificationType = emptyList(), slackConfiguration = SlackNotificationConfiguration(webhook = "")),
        sendOnConnectionUpdate = NotificationItem(notificationType = emptyList(), slackConfiguration = SlackNotificationConfiguration(webhook = "")),
        sendOnSyncDisabled = NotificationItem(notificationType = emptyList(), slackConfiguration = SlackNotificationConfiguration(webhook = "")),
        sendOnSyncDisabledWarning =
          NotificationItem(
            notificationType = emptyList(),
            slackConfiguration = SlackNotificationConfiguration(webhook = ""),
          ),
        sendOnConnectionUpdateActionRequired =
          NotificationItem(
            notificationType = emptyList(),
            slackConfiguration = SlackNotificationConfiguration(webhook = ""),
          ),
        sendOnConnectionSyncQueued =
          NotificationItem(
            notificationType = emptyList(),
            slackConfiguration = SlackNotificationConfiguration(webhook = ""),
          ),
      )

    private val EMPTY_CONFIG_NOTIFICATION_SETTINGS: io.airbyte.config.NotificationSettings? = emptyConfigNotificationSettings(null)

    // The API model requires a webhook, so converting an empty API settings object yields an empty string rather than null.
    private val EMPTY_WEBHOOK_CONFIG_NOTIFICATION_SETTINGS: io.airbyte.config.NotificationSettings? = emptyConfigNotificationSettings("")

    private fun emptyConfigNotificationSettings(webhook: String?): io.airbyte.config.NotificationSettings {
      fun emptyItem() =
        io.airbyte.config
          .NotificationItem()
          .withNotificationType(mutableListOf<Notification.NotificationType?>())
          .withSlackConfiguration(
            io.airbyte.config
              .SlackNotificationConfiguration()
              .withWebhook(webhook),
          )

      return io.airbyte.config
        .NotificationSettings()
        .withSendOnSuccess(emptyItem())
        .withSendOnFailure(emptyItem())
        .withSendOnConnectionUpdate(emptyItem())
        .withSendOnSyncDisabled(emptyItem())
        .withSendOnSyncDisabledWarning(emptyItem())
        .withSendOnConnectionUpdateActionRequired(emptyItem())
        .withSendOnConnectionSyncQueued(emptyItem())
    }
  }
}
