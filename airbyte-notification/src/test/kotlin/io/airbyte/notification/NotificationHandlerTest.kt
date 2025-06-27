/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

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

  private val webhookConfig: WebhookConfig = WebhookConfig("http://webhook.com")
  private val customerIoConfig: CustomerIoEmailConfig = CustomerIoEmailConfig("to@to.com")
  private val subject: String = "subject"
  private val message: String = "message"
  private val connectionId: UUID = UUID.randomUUID()

  @Test
  fun testNoBeanPresent() {
    val notificationHandler =
      NotificationHandler(
        maybeWebhookConfigFetcher = null,
        maybeCustomerIoConfigFetcher = null,
        maybeWebhookNotificationSender = null,
        maybeCustomerIoNotificationSender = null,
      )

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

    justRun { webhookNotificationSender.sendNotification(any(), any(), any(), any()) }
    justRun { customerIoNotificationSender.sendNotification(any(), any(), any(), any()) }

    notificationHandler.sendNotification(connectionId, subject, message, listOf(NotificationType.WEBHOOK, NotificationType.CUSTOMERIO))

    verify {
      webhookConfigFetcher.fetchConfig(connectionId)
      customerIoConfigFetcher.fetchConfig(connectionId)
      webhookNotificationSender.sendNotification(webhookConfig, subject, message, null)
      customerIoNotificationSender.sendNotification(customerIoConfig, subject, message, null)
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
