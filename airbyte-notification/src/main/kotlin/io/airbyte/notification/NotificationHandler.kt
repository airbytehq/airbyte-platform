package io.airbyte.notification

import jakarta.inject.Singleton
import java.util.UUID

enum class NotificationType {
    webhook, customerio
}

enum class NotificationEvent {
    onNonBreakingChange, onBreakingChange
}

@Singleton
open class NotificationHandler(
        private val maybeWebhookConfigFetcher: WebhookConfigFetcher?,
        private val maybeCustomerIoConfigFetcher: CustomerIoEmailConfigFetcher?,
        private val maybeWebhookNotificationSender: WebhookNotificationSender?,
        private val maybeCustomerIoNotificationSender: CustomerIoEmailNotificationSender?,
        private val maybeWorkspaceNotificationConfigFetcher: WorkspaceNotificationConfigFetcher?,
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

    open fun sendNotification(connectionId: UUID, subject: String, message: String, notificationEvent: NotificationEvent) {
        val notificationItemWithCustomerIoEmailConfig = maybeWorkspaceNotificationConfigFetcher?.fetchNotificationConfig(connectionId, notificationEvent)

        if (notificationItemWithCustomerIoEmailConfig?.notificationItem == null) {
            return;
        }
        notificationItemWithCustomerIoEmailConfig!!.notificationItem.notificationType!!.forEach { notificationType ->
            runCatching {
                if (maybeWebhookNotificationSender != null
                        && notificationType == io.airbyte.api.client.model.generated.NotificationType.SLACK) {
                    val webhookConfig = WebhookConfig(
                        notificationItemWithCustomerIoEmailConfig
                            !!.notificationItem
                            !!.slackConfiguration
                            !!.webhook)
                    maybeWebhookNotificationSender.sendNotification(webhookConfig, subject, message)
                }

                if (maybeCustomerIoNotificationSender != null
                    && notificationType == io.airbyte.api.client.model.generated.NotificationType.CUSTOMERIO) {
                        maybeCustomerIoNotificationSender
                            .sendNotification(notificationItemWithCustomerIoEmailConfig
                            !!.customerIoEmailConfig, subject, message)
                }
            }
        }
    }
}

