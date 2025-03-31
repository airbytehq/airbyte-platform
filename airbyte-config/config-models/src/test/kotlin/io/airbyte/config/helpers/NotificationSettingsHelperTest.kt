/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers

import io.airbyte.config.Notification
import io.airbyte.config.NotificationItem
import io.airbyte.config.NotificationSettings
import io.airbyte.config.SlackNotificationConfiguration
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class NotificationSettingsHelperTest {
  @Test
  fun testPatchNotificationSettingsWithDefaultValue() {
    val emailOnly = listOf(Notification.NotificationType.CUSTOMERIO)
    val slackOnly = listOf(Notification.NotificationType.SLACK)

    fun slack() =
      NotificationItem()
        .withNotificationType(listOf(Notification.NotificationType.SLACK))
        .withSlackConfiguration(SlackNotificationConfiguration().withWebhook("http://foo"))

    var n = patchNotificationSettingsWithDefaultValue(NotificationSettings())

    assertEquals(emptyList<Notification.NotificationType>(), n.sendOnSuccess.notificationType)
    assertEquals(emailOnly, n.sendOnFailure.notificationType)
    assertEquals(emailOnly, n.sendOnSyncDisabled.notificationType)
    assertEquals(emailOnly, n.sendOnSyncDisabledWarning.notificationType)
    assertEquals(emailOnly, n.sendOnConnectionUpdate.notificationType)
    assertEquals(emailOnly, n.sendOnConnectionUpdateActionRequired.notificationType)
    assertEquals(emailOnly, n.sendOnBreakingChangeWarning.notificationType)
    assertEquals(emailOnly, n.sendOnBreakingChangeSyncsDisabled.notificationType)

    n =
      patchNotificationSettingsWithDefaultValue(
        NotificationSettings()
          .withSendOnFailure(slack())
          .withSendOnSuccess(slack())
          .withSendOnSyncDisabled(slack())
          .withSendOnSyncDisabledWarning(slack())
          .withSendOnConnectionUpdate(slack())
          .withSendOnConnectionUpdateActionRequired(slack())
          .withSendOnBreakingChangeWarning(slack())
          .withSendOnBreakingChangeSyncsDisabled(slack()),
      )

    assertEquals(slackOnly, n.sendOnSuccess.notificationType)
    assertEquals(slackOnly, n.sendOnFailure.notificationType)
    assertEquals(slackOnly, n.sendOnSyncDisabled.notificationType)
    assertEquals(slackOnly, n.sendOnSyncDisabledWarning.notificationType)
    assertEquals(slackOnly, n.sendOnConnectionUpdate.notificationType)
    assertEquals(slackOnly, n.sendOnConnectionUpdateActionRequired.notificationType)
    assertEquals(slackOnly, n.sendOnBreakingChangeWarning.notificationType)
    assertEquals(slackOnly, n.sendOnBreakingChangeSyncsDisabled.notificationType)
  }
}
