package io.airbyte.notification

import jakarta.inject.Singleton
import java.util.UUID

enum class NotificationType {
    webhook, customerio
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
    open fun sendNotification(connectionId: UUID, title: String, message: String, notificationTypes: List<NotificationType>) {
        notificationTypes.forEach { notificationType ->
            runCatching {
                if (maybeWebhookConfigFetcher != null && maybeWebhookNotificationSender != null && notificationType == NotificationType.webhook) {
                    maybeWebhookConfigFetcher.fetchConfig(connectionId)?.let {
                        maybeWebhookNotificationSender.sendNotification(it, title, message)
                    }
                }

                if (maybeCustomerIoConfigFetcher != null && maybeCustomerIoNotificationSender != null && notificationType == NotificationType.customerio) {
                    maybeCustomerIoConfigFetcher.fetchConfig(connectionId)?.let {
                        maybeCustomerIoNotificationSender.sendNotification(it, title, message)
                    }
                }
            }
        }
    }
}