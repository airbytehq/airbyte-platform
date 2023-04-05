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
    private val sendGridEmailConfigFetcher: SendGridEmailConfigFetcher = mockk()
    private val webhookConfigFetcher: WebhookConfigFetcher = mockk()
    private val sendGridEmailNotificationSender: SendGridEmailNotificationSender = mockk()
    private val webhookNotificationSender: WebhookNotificationSender = mockk()

    private val sendGridEmailConfig: SendGridEmailConfig = SendGridEmailConfig("from@from.com", "to@to.com")
    private val webhookConfig: WebhookConfig = WebhookConfig("http://webhook.com")
    private val subject: String = "subject"
    private val message: String = "message"
    private val connectionId: UUID = UUID.randomUUID()

    @Test
    fun testNoBeanPresent() {
        val notificationHandler = NotificationHandler(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())

        notificationHandler.sendNotification(connectionId, subject, message, listOf(NotificationType.email, NotificationType.webhook))

        verify {
            sendGridEmailConfigFetcher wasNot called
            webhookConfigFetcher wasNot called
            sendGridEmailNotificationSender wasNot called
            webhookNotificationSender wasNot called
        }
    }

    @Test
    fun testAllNotification() {
        val notificationHandler = NotificationHandler(Optional.of(sendGridEmailConfigFetcher),
                Optional.of(webhookConfigFetcher),
                Optional.of(sendGridEmailNotificationSender),
                Optional.of(webhookNotificationSender))

        every {
            sendGridEmailConfigFetcher.fetchConfig(connectionId)
        } answers {
            sendGridEmailConfig
        }

        every {
            webhookConfigFetcher.fetchConfig(connectionId)
        } answers {
            webhookConfig
        }

        justRun { sendGridEmailNotificationSender.sendNotification(any(), any(), any()) }
        justRun { webhookNotificationSender.sendNotification(any(), any(), any()) }

        notificationHandler.sendNotification(connectionId, subject, message, listOf(NotificationType.email, NotificationType.webhook))

        verify {
            sendGridEmailConfigFetcher.fetchConfig(connectionId)
            webhookConfigFetcher.fetchConfig(connectionId)
            sendGridEmailNotificationSender.sendNotification(sendGridEmailConfig, subject, message)
            webhookNotificationSender.sendNotification(webhookConfig, subject, message)
        }
    }

    @Test
    fun testPartialNotification() {
        val notificationHandler = NotificationHandler(Optional.of(sendGridEmailConfigFetcher),
                Optional.of(webhookConfigFetcher),
                Optional.of(sendGridEmailNotificationSender),
                Optional.of(webhookNotificationSender))

        every {
            sendGridEmailConfigFetcher.fetchConfig(connectionId)
        } answers {
            sendGridEmailConfig
        }

        justRun { sendGridEmailNotificationSender.sendNotification(any(), any(), any()) }

        notificationHandler.sendNotification(connectionId, subject, message, listOf(NotificationType.email))

        verify {
            sendGridEmailConfigFetcher.fetchConfig(connectionId)
            webhookConfigFetcher wasNot called
            sendGridEmailNotificationSender.sendNotification(sendGridEmailConfig, subject, message)
            webhookNotificationSender wasNot called
        }
    }
}