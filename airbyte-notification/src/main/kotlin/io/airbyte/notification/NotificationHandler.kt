package io.airbyte.notification

import jakarta.inject.Singleton
import java.util.*

enum class NotificationType {
    webhook
}

@Singleton
class NotificationHandler(private val maybeWebhookConfigFetcher: Optional<WebhookConfigFetcher>,
                          private val maybeWebhookNotificationSender: Optional<WebhookNotificationSender>) {

    /**
     * Send a notification with a subject and a message if a configuration is present
     */
    fun sendNotification(connectionId: UUID, title: String, message: String, notificationTypes: List<NotificationType>) {
        notificationTypes.forEach { notificationType ->
            runCatching {
                if (maybeWebhookConfigFetcher.isPresent && maybeWebhookNotificationSender.isPresent && notificationType == NotificationType.webhook) {
                    val config: WebhookConfig? = maybeWebhookConfigFetcher.get().fetchConfig(connectionId)
                    if (config != null) {
                        maybeWebhookNotificationSender.get().sendNotification(config, title, message)
                    }
                }
            }
        }
    }
}