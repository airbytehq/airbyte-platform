package io.airbyte.notification

import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import java.util.Optional
import java.util.UUID

enum class NotificationType {
    webhook
}

@Singleton
class NotificationHandler(private val maybeWebhookConfigFetcher: WebhookConfigFetcher?,
                          private val maybeWebhookNotificationSender: WebhookNotificationSender?) {

    /**
     * Send a notification with a subject and a message if a configuration is present
     */
    fun sendNotification(connectionId: UUID, title: String, message: String, notificationTypes: List<NotificationType>) {
        notificationTypes.forEach { notificationType ->
            runCatching {
                if (maybeWebhookConfigFetcher != null && maybeWebhookNotificationSender != null && notificationType == NotificationType.webhook) {
                    val config: WebhookConfig? = maybeWebhookConfigFetcher.fetchConfig(connectionId)
                    if (config != null) {
                        maybeWebhookNotificationSender.sendNotification(config, title, message)
                    }
                }
            }
        }
    }
}