package io.airbyte.notification

import io.mockk.called
import io.mockk.every
import io.mockk.justRun
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID

class NotificationHandlerTest {
    private val webhookConfigFetcher: WebhookConfigFetcher = mockk()
    private val webhookNotificationSender: WebhookNotificationSender = mockk()

    private val webhookConfig: WebhookConfig = WebhookConfig("http://webhook.com")
    private val subject: String = "subject"
    private val message: String = "message"
    private val connectionId: UUID = UUID.randomUUID()

    @Test
    fun testNoBeanPresent() {
        val notificationHandler = NotificationHandler(null, null)

        notificationHandler.sendNotification(connectionId, subject, message, listOf(NotificationType.webhook))

        verify {
            webhookConfigFetcher wasNot called
            webhookNotificationSender wasNot called
        }
    }

    @Test
    fun testAllNotification() {
        val notificationHandler = NotificationHandler(webhookConfigFetcher, webhookNotificationSender)

        every {
            webhookConfigFetcher.fetchConfig(connectionId)
        } answers {
            webhookConfig
        }

        justRun { webhookNotificationSender.sendNotification(any(), any(), any()) }

        notificationHandler.sendNotification(connectionId, subject, message, listOf(NotificationType.webhook))

        verify {
            webhookConfigFetcher.fetchConfig(connectionId)
            webhookNotificationSender.sendNotification(webhookConfig, subject, message)
        }
    }

    @Test
    fun testPartialNotification() {
        val notificationHandler = NotificationHandler(webhookConfigFetcher, webhookNotificationSender)

        notificationHandler.sendNotification(connectionId, subject, message, listOf())

        verify {
            webhookConfigFetcher wasNot called
            webhookNotificationSender wasNot called
        }
    }
}