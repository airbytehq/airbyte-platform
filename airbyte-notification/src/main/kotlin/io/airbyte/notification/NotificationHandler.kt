/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification

import jakarta.inject.Singleton
import java.util.UUID

enum class NotificationType {
  WEBHOOK,
  CUSTOMERIO,
}

enum class NotificationEvent {
  ON_NON_BREAKING_CHANGE,
  ON_BREAKING_CHANGE,
}

@Singleton
open class NotificationHandler(
  private val maybeWebhookConfigFetcher: WebhookConfigFetcher?,
  private val maybeCustomerIoConfigFetcher: CustomerIoEmailConfigFetcher?,
  private val maybeWebhookNotificationSender: WebhookNotificationSender?,
  private val maybeCustomerIoNotificationSender: CustomerIoEmailNotificationSender?,
) {
  /**
   * Send a notification with a subject and a message if a configuration is present
   */
  open fun sendNotification(
    connectionId: UUID,
    title: String,
    message: String,
    notificationTypes: List<NotificationType>,
  ) {
    notificationTypes.forEach { notificationType ->
      runCatching {
        if (maybeWebhookConfigFetcher != null && maybeWebhookNotificationSender != null && notificationType == NotificationType.WEBHOOK) {
          maybeWebhookConfigFetcher.fetchConfig(connectionId)?.let {
            maybeWebhookNotificationSender.sendNotification(it, title, message, null) // TODO
          }
        }

        if (maybeCustomerIoConfigFetcher != null && maybeCustomerIoNotificationSender != null && notificationType == NotificationType.CUSTOMERIO) {
          maybeCustomerIoConfigFetcher.fetchConfig(connectionId)?.let {
            maybeCustomerIoNotificationSender.sendNotification(it, title, message, null) // TODO
          }
        }
      }
    }
  }
}
