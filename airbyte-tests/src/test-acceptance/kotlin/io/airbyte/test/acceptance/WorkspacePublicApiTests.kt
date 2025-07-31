/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance

import io.airbyte.api.client.generated.PublicWorkspacesApi
import io.airbyte.api.client.model.generated.EmailNotificationConfig
import io.airbyte.api.client.model.generated.NotificationConfig
import io.airbyte.api.client.model.generated.NotificationsConfig
import io.airbyte.api.client.model.generated.WebhookNotificationConfig
import io.airbyte.api.client.model.generated.WorkspaceCreateRequest
import io.airbyte.api.client.model.generated.WorkspaceUpdateRequest
import io.airbyte.api.problems.model.generated.NotificationMissingUrlProblemResponse
import io.airbyte.api.problems.model.generated.NotificationRequiredProblemResponse
import io.airbyte.commons.json.Jsons
import io.airbyte.test.utils.AcceptanceTestUtils
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.openapitools.client.infrastructure.ClientError

@Tag("api")
class WorkspacePublicApiTests {
  val testResources = AcceptanceTestsResources()
  val apiClient =
    PublicWorkspacesApi(
      basePath = AcceptanceTestUtils.getAirbyteApiUrl(),
      client = AcceptanceTestUtils.createOkHttpClient(),
    )

  @BeforeEach
  @Throws(Exception::class)
  fun setup() {
    testResources.init()
    testResources.setup()
  }

  @AfterEach
  fun tearDown() {
    testResources.tearDown()
    testResources.end()
  }

  @Test
  fun getWorkspaceDefaultNotificationsConfig() {
    val resp = apiClient.publicGetWorkspace(testResources.workspaceId.toString())
    val expect =
      NotificationsConfig(
        failure =
          NotificationConfig(
            email = EmailNotificationConfig(enabled = true),
            webhook = WebhookNotificationConfig(enabled = false),
          ),
        success =
          NotificationConfig(
            email = EmailNotificationConfig(enabled = false),
            webhook = WebhookNotificationConfig(enabled = false),
          ),
        connectionUpdate =
          NotificationConfig(
            email = EmailNotificationConfig(enabled = true),
            webhook = WebhookNotificationConfig(enabled = false),
          ),
        connectionUpdateActionRequired =
          NotificationConfig(
            email = EmailNotificationConfig(enabled = true),
            webhook = WebhookNotificationConfig(enabled = false),
          ),
        syncDisabled =
          NotificationConfig(
            email = EmailNotificationConfig(enabled = true),
            webhook = WebhookNotificationConfig(enabled = false),
          ),
        syncDisabledWarning =
          NotificationConfig(
            email = EmailNotificationConfig(enabled = true),
            webhook = WebhookNotificationConfig(enabled = false),
          ),
      )
    assertEquals(expect, resp.notifications)
  }

  @Test
  fun createWorkspace() {
    val resp = apiClient.publicCreateWorkspace(WorkspaceCreateRequest("test"))
    assertEquals("test", resp.name)
  }

  @Test
  fun updateWorkspace() {
    val workspaceId = testResources.workspaceId.toString()
    val resp = apiClient.publicUpdateWorkspace(workspaceId, WorkspaceUpdateRequest("test2"))
    assertEquals("test2", resp.name)
  }

  @Test
  fun updateWorkspaceNotifications() {
    val workspaceId = testResources.workspaceId.toString()
    val resp =
      apiClient.publicUpdateWorkspace(
        workspaceId,
        WorkspaceUpdateRequest(
          notifications =
            NotificationsConfig(
              failure =
                NotificationConfig(
                  webhook = WebhookNotificationConfig(enabled = true, url = "http://test.airbyte"),
                ),
            ),
        ),
      )
    assertEquals(
      resp.notifications.success
        ?.webhook
        ?.enabled,
      false,
    )
    assertEquals(
      resp.notifications.success
        ?.webhook
        ?.url,
      null,
    )
    assertEquals(
      resp.notifications.failure
        ?.webhook
        ?.enabled,
      true,
    )
    assertEquals(
      resp.notifications.failure
        ?.webhook
        ?.url,
      "http://test.airbyte",
    )

    val resp2 =
      apiClient.publicUpdateWorkspace(
        workspaceId,
        WorkspaceUpdateRequest(
          notifications =
            NotificationsConfig(
              success =
                NotificationConfig(
                  webhook = WebhookNotificationConfig(enabled = true, url = "http://success.airbyte"),
                ),
            ),
        ),
      )
    assertEquals(
      resp2.notifications.failure
        ?.webhook
        ?.enabled,
      true,
    )
    assertEquals(
      resp2.notifications.failure
        ?.webhook
        ?.url,
      "http://test.airbyte",
    )
    assertEquals(
      resp2.notifications.success
        ?.webhook
        ?.enabled,
      true,
    )
    assertEquals(
      resp2.notifications.success
        ?.webhook
        ?.url,
      "http://success.airbyte",
    )
  }

  @Test
  fun createWorkspaceWithInvalidNotificationsConfig() {
    val config =
      NotificationsConfig(
        failure =
          NotificationConfig(
            webhook = WebhookNotificationConfig(enabled = true),
          ),
      )

    val exception =
      assertThrows<org.openapitools.client.infrastructure.ClientException> {
        apiClient.publicCreateWorkspace(WorkspaceCreateRequest("test", null, config))
      }

    val err = exception.response as ClientError<*>
    val prob = Jsons.deserialize(err.body as String, NotificationMissingUrlProblemResponse::class.java)
    assertEquals(400, exception.statusCode)
    assertEquals(400, prob.getStatus())
    assertEquals("The 'failure' notification is enabled but is missing a URL.", prob.getData()?.message)
  }

  @Test
  fun invalidWebhookUpdate() {
    val workspaceId = testResources.workspaceId.toString()
    val config =
      NotificationsConfig(
        failure =
          NotificationConfig(
            webhook = WebhookNotificationConfig(enabled = true),
          ),
      )
    val exception =
      assertThrows<org.openapitools.client.infrastructure.ClientException> {
        apiClient.publicUpdateWorkspace(workspaceId, WorkspaceUpdateRequest(notifications = config))
      }

    val err = exception.response as ClientError<*>
    val prob = Jsons.deserialize(err.body as String, NotificationRequiredProblemResponse::class.java)
    assertEquals(400, exception.statusCode)
    assertEquals(400, prob.getStatus())
    assertEquals("The 'failure' notification is enabled but is missing a URL.", prob.getData()?.message)
  }
}
