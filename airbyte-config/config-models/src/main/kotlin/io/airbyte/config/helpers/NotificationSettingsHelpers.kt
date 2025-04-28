/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers

import io.airbyte.config.Notification
import io.airbyte.config.NotificationItem
import io.airbyte.config.NotificationSettings

private fun emailNotificationItem() = NotificationItem().withNotificationType(listOf(Notification.NotificationType.CUSTOMERIO))

fun patchNotificationSettingsWithDefaultValue(notificationSettings: NotificationSettings?): NotificationSettings {
  val ns = notificationSettings ?: NotificationSettings()
  ns.sendOnSuccess = ns.sendOnSuccess ?: NotificationItem()
  ns.sendOnFailure = ns.sendOnFailure ?: emailNotificationItem()
  ns.sendOnSyncDisabled = ns.sendOnSyncDisabled ?: emailNotificationItem()
  ns.sendOnSyncDisabledWarning = ns.sendOnSyncDisabledWarning ?: emailNotificationItem()
  ns.sendOnConnectionUpdate = ns.sendOnConnectionUpdate ?: emailNotificationItem()
  ns.sendOnConnectionUpdateActionRequired = ns.sendOnConnectionUpdateActionRequired ?: emailNotificationItem()
  ns.sendOnBreakingChangeWarning = ns.sendOnBreakingChangeWarning ?: emailNotificationItem()
  ns.sendOnBreakingChangeSyncsDisabled = ns.sendOnBreakingChangeSyncsDisabled ?: emailNotificationItem()
  return ns
}
