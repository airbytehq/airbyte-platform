/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging.provider

import io.airbyte.api.model.generated.WebhookConfigRead
import io.airbyte.api.model.generated.WebhookConfigWrite
import io.airbyte.api.model.generated.WorkspaceCreate
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.api.model.generated.WorkspaceRead
import io.airbyte.api.model.generated.WorkspaceUpdate
import io.airbyte.api.model.generated.WorkspaceUpdateName
import io.airbyte.api.model.generated.WorkspaceUpdateOrganization
import io.airbyte.commons.json.Jsons
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test
import java.util.UUID

class WorkspaceAuditProviderTest {
  private val provider = WorkspaceAuditProvider()

  @Test
  fun `createWorkspace request summary omits the webhook authToken but keeps identity`() {
    val organizationId = UUID.randomUUID()
    val request =
      WorkspaceCreate()
        .name("my workspace")
        .email("owner@example.com")
        .organizationId(organizationId)
        .webhookConfigs(listOf(WebhookConfigWrite().name("hook").authToken("super-secret-value")))

    val summary = provider.generateSummaryFromRequest(request)

    assertFalse(summary.contains("super-secret-value"), "authToken value leaked into audit summary: $summary")
    assertFalse(summary.contains("authToken"), "authToken key leaked into audit summary: $summary")
    assertFalse(summary.contains("webhookConfigs"), "webhookConfigs key leaked into audit summary: $summary")

    val node = Jsons.deserialize(summary)
    assertEquals("my workspace", node.get("name").asText())
    assertEquals("owner@example.com", node.get("email").asText())
    assertEquals(organizationId.toString(), node.get("organizationId").asText())
  }

  @Test
  fun `createWorkspace response summary carries the newly assigned workspaceId`() {
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val result =
      WorkspaceRead()
        .workspaceId(workspaceId)
        .name("my workspace")
        .email("owner@example.com")
        .organizationId(organizationId)
        .webhookConfigs(listOf(WebhookConfigRead().id(UUID.randomUUID()).name("hook")))

    val summary = provider.generateSummaryFromResult(result)

    // WebhookConfigRead has no authToken, but the response is allowlisted the same way so the
    // preference and webhook state stays out of the audit trail.
    assertFalse(summary.contains("webhookConfigs"), "webhookConfigs key leaked into audit summary: $summary")

    val node = Jsons.deserialize(summary)
    assertEquals(workspaceId.toString(), node.get("workspaceId").asText())
    assertEquals(organizationId.toString(), node.get("organizationId").asText())
  }

  @Test
  fun `updateWorkspace request summary omits the webhook authToken`() {
    val workspaceId = UUID.randomUUID()
    val request =
      WorkspaceUpdate()
        .workspaceId(workspaceId)
        .name("renamed")
        .email("owner@example.com")
        .webhookConfigs(listOf(WebhookConfigWrite().name("hook").authToken("super-secret-value")))

    val summary = provider.generateSummaryFromRequest(request)

    assertFalse(summary.contains("super-secret-value"), "authToken value leaked into audit summary: $summary")

    val node = Jsons.deserialize(summary)
    assertEquals(workspaceId.toString(), node.get("workspaceId").asText())
    assertEquals("renamed", node.get("name").asText())
    assertEquals("owner@example.com", node.get("email").asText())
  }

  @Test
  fun `updateWorkspaceName request summary identifies the workspace and its new name`() {
    val workspaceId = UUID.randomUUID()

    val node = Jsons.deserialize(provider.generateSummaryFromRequest(WorkspaceUpdateName().workspaceId(workspaceId).name("renamed")))

    assertEquals(workspaceId.toString(), node.get("workspaceId").asText())
    assertEquals("renamed", node.get("name").asText())
  }

  @Test
  fun `updateWorkspaceOrganization request summary identifies the target organization`() {
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()

    val node =
      Jsons.deserialize(
        provider.generateSummaryFromRequest(
          WorkspaceUpdateOrganization().workspaceId(workspaceId).organizationId(organizationId),
        ),
      )

    assertEquals(workspaceId.toString(), node.get("workspaceId").asText())
    assertEquals(organizationId.toString(), node.get("organizationId").asText())
  }

  @Test
  fun `deleteWorkspace request summary identifies which workspace was deleted`() {
    val workspaceId = UUID.randomUUID()

    val node = Jsons.deserialize(provider.generateSummaryFromRequest(WorkspaceIdRequestBody().workspaceId(workspaceId)))

    assertEquals(workspaceId.toString(), node.get("workspaceId").asText())
  }

  @Test
  fun `null request and result yield an empty summary`() {
    assertEquals(AuditProvider.EMPTY_SUMMARY, provider.generateSummaryFromRequest(null))
    assertEquals(AuditProvider.EMPTY_SUMMARY, provider.generateSummaryFromResult(null))
  }
}
