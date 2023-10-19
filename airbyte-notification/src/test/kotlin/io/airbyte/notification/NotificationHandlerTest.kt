package io.airbyte.notification

import io.mockk.called
import io.mockk.every
import io.mockk.justRun
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.util.UUID

class NotificationHandlerTest {
  private val webhookConfigFetcher: WebhookConfigFetcher = mockk()
  private val customerIoConfigFetcher: CustomerIoEmailConfigFetcher = mockk()
  private val webhookNotificationSender: WebhookNotificationSender = mockk()
  private val customerIoNotificationSender: CustomerIoEmailNotificationSender = mockk()
  private val workspaceNotificationConfigFetcher: WorkspaceNotificationConfigFetcher = mockk()

  private val webhookConfig: WebhookConfig = WebhookConfig("http://webhook.com")
  private val customerIoConfig: CustomerIoEmailConfig = CustomerIoEmailConfig("to@to.com")
  private val subject: String = "subject"
  private val message: String = "message"
  private val connectionId: UUID = UUID.randomUUID()

  @Test
  fun testNoBeanPresent() {
    val notificationHandler = NotificationHandler(null, null, null, null, null)

    notificationHandler.sendNotification(connectionId, subject, message, listOf(NotificationType.WEBHOOK))

    verify {
      webhookConfigFetcher wasNot called
      webhookNotificationSender wasNot called
    }
  }

  @Test
  fun testAllNotification() {
    val notificationHandler =
      NotificationHandler(
        webhookConfigFetcher,
        customerIoConfigFetcher,
        webhookNotificationSender,
        customerIoNotificationSender,
        workspaceNotificationConfigFetcher,
      )

    every {
      webhookConfigFetcher.fetchConfig(connectionId)
    } answers {
      webhookConfig
    }

    every {
      customerIoConfigFetcher.fetchConfig(connectionId)
    } answers {
      customerIoConfig
    }

    justRun { webhookNotificationSender.sendNotification(any(), any(), any()) }
    justRun { customerIoNotificationSender.sendNotification(any(), any(), any()) }

    notificationHandler.sendNotification(connectionId, subject, message, listOf(NotificationType.WEBHOOK, NotificationType.CUSTOMERIO))

    verify {
      webhookConfigFetcher.fetchConfig(connectionId)
      customerIoConfigFetcher.fetchConfig(connectionId)
      webhookNotificationSender.sendNotification(webhookConfig, subject, message)
      customerIoNotificationSender.sendNotification(customerIoConfig, subject, message)
    }
  }

  @Test
  fun testPartialNotification() {
    val notificationHandler =
      NotificationHandler(
        webhookConfigFetcher,
        customerIoConfigFetcher,
        webhookNotificationSender,
        customerIoNotificationSender,
        workspaceNotificationConfigFetcher,
      )

    notificationHandler.sendNotification(connectionId, subject, message, listOf())

    verify {
      webhookConfigFetcher wasNot called
      customerIoConfigFetcher wasNot called
      webhookNotificationSender wasNot called
      customerIoNotificationSender wasNot called
    }
  }
}
