/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance

import io.airbyte.api.client.model.generated.EmailNotificationConfig
import io.airbyte.api.client.model.generated.NotificationConfig
import io.airbyte.api.client.model.generated.NotificationsConfig
import io.airbyte.api.client.model.generated.WebhookNotificationConfig
import io.airbyte.api.client.model.generated.WorkspaceCreateRequest
import io.airbyte.api.client.model.generated.WorkspaceUpdateRequest
import io.airbyte.api.problems.model.generated.NotificationMissingUrlProblemResponse
import io.airbyte.api.problems.model.generated.NotificationRequiredProblemResponse
import io.airbyte.commons.json.Jsons
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import org.openapitools.client.infrastructure.ClientError
import org.openapitools.client.infrastructure.ClientException

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("api")
internal class WorkspacePublicApiTest {
  private val atClient = AcceptanceTestClient()

  @BeforeAll
  fun setup() {
    atClient.setup()
  }

  @AfterAll
  fun tearDownAll() {
    atClient.tearDownAll()
  }

  @AfterEach
  fun tearDown() {
    atClient.tearDown()
  }

  @Test
  fun getWorkspaceDefaultNotificationsConfig() {
    val workspaceId = atClient.public.createWorkspace(WorkspaceCreateRequest(name = "test"))
    val resp = atClient.public.fetchWorkspace(workspaceId)
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
    val workspaceId = atClient.public.createWorkspace(WorkspaceCreateRequest(name = "test"))
    val workspace = atClient.public.fetchWorkspace(workspaceId)
    assertEquals("test", workspace.name)
  }

  @Test
  fun updateWorkspace() {
    val workspaceId = atClient.public.createWorkspace(WorkspaceCreateRequest(name = "test"))
    val updatedWorkspaceId = atClient.public.updateWorkspace(workspaceId, WorkspaceUpdateRequest(name = "test2"))
    assertEquals(workspaceId, updatedWorkspaceId)

    val updatedWorkspace = atClient.public.fetchWorkspace(workspaceId)
    assertEquals("test2", updatedWorkspace.name)
  }

  @Test
  fun updateWorkspaceNotifications() {
    val workspaceId = atClient.public.createWorkspace(WorkspaceCreateRequest(name = "test"))

    atClient.public.updateWorkspace(
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

    val updatedWorkspace1 = atClient.public.fetchWorkspace(workspaceId)
    assertEquals(
      updatedWorkspace1.notifications.success
        ?.webhook
        ?.enabled,
      false,
    )
    assertEquals(
      updatedWorkspace1.notifications.success
        ?.webhook
        ?.url,
      null,
    )
    assertEquals(
      updatedWorkspace1.notifications.failure
        ?.webhook
        ?.enabled,
      true,
    )
    assertEquals(
      updatedWorkspace1.notifications.failure
        ?.webhook
        ?.url,
      "http://test.airbyte",
    )

    atClient.public.updateWorkspace(
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

    val updatedWorkspace2 = atClient.public.fetchWorkspace(workspaceId)
    assertEquals(
      updatedWorkspace2.notifications.failure
        ?.webhook
        ?.enabled,
      true,
    )
    assertEquals(
      updatedWorkspace2.notifications.failure
        ?.webhook
        ?.url,
      "http://test.airbyte",
    )
    assertEquals(
      updatedWorkspace2.notifications.success
        ?.webhook
        ?.enabled,
      true,
    )
    assertEquals(
      updatedWorkspace2.notifications.success
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
      assertThrows<ClientException> {
        atClient.public.createWorkspace(WorkspaceCreateRequest(name = "test", notifications = config))
      }

    val err = exception.response as ClientError<*>
    val prob = Jsons.deserialize(err.body as String, NotificationMissingUrlProblemResponse::class.java)
    assertEquals(400, exception.statusCode)
    assertEquals(400, prob.getStatus())
    assertEquals("The 'failure' notification is enabled but is missing a URL.", prob.getData()?.message)
  }

  @Test
  fun invalidWebhookUpdate() {
    val workspaceId = atClient.public.createWorkspace(WorkspaceCreateRequest(name = "test"))
    val config =
      NotificationsConfig(
        failure = NotificationConfig(webhook = WebhookNotificationConfig(enabled = true)),
      )
    val exception =
      assertThrows<ClientException> {
        atClient.public.updateWorkspace(workspaceId, WorkspaceUpdateRequest(notifications = config))
      }

    val err = exception.response as ClientError<*>
    val prob = Jsons.deserialize(err.body as String, NotificationRequiredProblemResponse::class.java)
    assertEquals(400, exception.statusCode)
    assertEquals(400, prob.getStatus())
    assertEquals("The 'failure' notification is enabled but is missing a URL.", prob.getData()?.message)
  }
}
