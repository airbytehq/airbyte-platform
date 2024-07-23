package io.airbyte.notification

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.WorkspaceApi
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.Notification
import io.airbyte.api.client.model.generated.NotificationType
import io.airbyte.api.client.model.generated.SlackNotificationConfiguration
import io.airbyte.api.client.model.generated.WorkspaceRead
import io.micronaut.http.HttpStatus
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.openapitools.client.infrastructure.ClientException
import java.util.UUID

internal class WebhookConfigFetcherTest {
  private val airbyteApiClient: AirbyteApiClient = mockk()
  private val workspaceApi: WorkspaceApi = mockk()
  private val connectionId: UUID = UUID.randomUUID()
  private val connectionIdRequestBody = ConnectionIdRequestBody(connectionId)
  private lateinit var webhookConfigFetcher: WebhookConfigFetcher

  @BeforeEach
  internal fun setup() {
    every { airbyteApiClient.workspaceApi } returns workspaceApi
    webhookConfigFetcher = WebhookConfigFetcher(airbyteApiClient = airbyteApiClient)
  }

  @Test
  fun testNoWorkspace() {
    every {
      workspaceApi.getWorkspaceByConnectionId(connectionIdRequestBody)
    } throws ClientException("Not found", HttpStatus.NOT_FOUND.code, null)

    assertThrows<ClientException> {
      webhookConfigFetcher.fetchConfig(connectionId)
    }
  }

  @Test
  fun testNoNotification() {
    val workspaceRead: WorkspaceRead = mockk()

    every {
      workspaceRead.notifications
    } returns listOf()

    every {
      workspaceApi.getWorkspaceByConnectionId(connectionIdRequestBody)
    } returns workspaceRead

    val webhookConfig: WebhookConfig? = webhookConfigFetcher.fetchConfig(connectionId)

    assertEquals(null, webhookConfig)
  }

  @Test
  fun testNoSlackNotification() {
    val notification: Notification = mockk()
    every { notification.notificationType } returns NotificationType.CUSTOMERIO

    val workspaceRead: WorkspaceRead = mockk()
    every { workspaceRead.notifications } returns listOf(notification)

    every {
      workspaceApi.getWorkspaceByConnectionId(connectionIdRequestBody)
    } returns workspaceRead

    val webhookConfig: WebhookConfig? = webhookConfigFetcher.fetchConfig(connectionId)

    assertEquals(null, webhookConfig)
  }

  @Test
  fun testSlackNotification() {
    val notification: Notification = mockk()
    val webhook = "http://webhook"

    every { notification.notificationType } returns NotificationType.SLACK
    every { notification.slackConfiguration } returns SlackNotificationConfiguration(webhook)

    val workspaceRead: WorkspaceRead = mockk()
    every { workspaceRead.notifications } returns listOf(notification)

    every {
      workspaceApi.getWorkspaceByConnectionId(connectionIdRequestBody)
    } returns workspaceRead

    val webhookConfig: WebhookConfig? = webhookConfigFetcher.fetchConfig(connectionId)

    val expected = WebhookConfig(webhook)

    assertEquals(expected, webhookConfig)
  }
}
