package io.airbyte.notification

import io.airbyte.api.client.generated.WorkspaceApi
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.Notification
import io.airbyte.api.client.model.generated.NotificationType
import io.airbyte.api.client.model.generated.SlackNotificationConfiguration
import io.airbyte.api.client.model.generated.WorkspaceRead
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.UUID

class WebhookConfigFetcherTest {
  private val workspaceApiClient: WorkspaceApi = mockk()
  private val connectionId: UUID = UUID.randomUUID()
  private val connectionIdRequestBody = ConnectionIdRequestBody().connectionId(connectionId)
  private val webhookConfigFetcher: WebhookConfigFetcher = WebhookConfigFetcher(workspaceApiClient)

  @Test
  fun testNoWorkspace() {
    every {
      workspaceApiClient.getWorkspaceByConnectionId(connectionIdRequestBody)
    } returns null

    val webhookConfig: WebhookConfig? = webhookConfigFetcher.fetchConfig(connectionId)

    Assertions.assertEquals(null, webhookConfig)
  }

  @Test
  fun testNoNotification() {
    val workspaceRead = WorkspaceRead()
    workspaceRead.notifications = null

    every {
      workspaceApiClient.getWorkspaceByConnectionId(connectionIdRequestBody)
    } returns workspaceRead

    val webhookConfig: WebhookConfig? = webhookConfigFetcher.fetchConfig(connectionId)

    Assertions.assertEquals(null, webhookConfig)
  }

  @Test
  fun testNoSlackNotification() {
    val notification = Notification()
    notification.notificationType = NotificationType.CUSTOMERIO
    val workspaceRead = WorkspaceRead()
    workspaceRead.notifications =
      listOf(
        notification,
      )

    every {
      workspaceApiClient.getWorkspaceByConnectionId(connectionIdRequestBody)
    } returns workspaceRead

    val webhookConfig: WebhookConfig? = webhookConfigFetcher.fetchConfig(connectionId)

    Assertions.assertEquals(null, webhookConfig)
  }

  @Test
  fun testSlackNotification() {
    val notification = Notification()
    notification.notificationType = NotificationType.SLACK
    val webhook = "http://webhook"
    notification.slackConfiguration = SlackNotificationConfiguration().webhook(webhook)
    val workspaceRead = WorkspaceRead()
    workspaceRead.notifications =
      listOf(
        notification,
      )

    every {
      workspaceApiClient.getWorkspaceByConnectionId(connectionIdRequestBody)
    } returns workspaceRead

    val webhookConfig: WebhookConfig? = webhookConfigFetcher.fetchConfig(connectionId)

    val expected = WebhookConfig(webhook)

    Assertions.assertEquals(expected, webhookConfig)
  }
}
