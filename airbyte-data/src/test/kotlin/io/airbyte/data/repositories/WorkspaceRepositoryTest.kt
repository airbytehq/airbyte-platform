/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Workspace
import io.airbyte.db.instance.configs.jooq.generated.Keys
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.util.UUID

@MicronautTest
class WorkspaceRepositoryTest : AbstractConfigRepositoryTest() {
  companion object {
    @BeforeAll
    @JvmStatic
    fun setup() {
      // so we don't have to deal with making orgs as well
      jooqDslContext
        .alterTable(
          Tables.WORKSPACE,
        ).dropForeignKey(Keys.WORKSPACE__WORKSPACE_ORGANIZATION_ID_FKEY.constraint())
        .execute()
    }
  }

  @Test
  fun `save and retrieve workspace by id`() {
    val workspace =
      Workspace(
        customerId = UUID.randomUUID(),
        name = "Test Workspace",
        slug = "test-workspace",
        email = "test@example.com",
        initialSetupComplete = true,
        anonymousDataCollection = false,
        sendNewsletter = true,
        displaySetupWizard = false,
        tombstone = false,
        notifications = null,
        firstSyncComplete = true,
        feedbackComplete = true,
        dataplaneGroupId = UUID.randomUUID(),
        webhookOperationConfigs = null,
        notificationSettings = null,
        organizationId = UUID.randomUUID(),
      )
    workspaceRepository.save(workspace)

    val retrievedWorkspace = workspaceRepository.findById(workspace.id!!)
    assertTrue(retrievedWorkspace.isPresent)
    assertThat(retrievedWorkspace.get())
      .usingRecursiveComparison()
      .ignoringFields("createdAt", "updatedAt")
      .isEqualTo(workspace)
  }

  @Test
  fun `update workspace`() {
    val workspace =
      Workspace(
        customerId = UUID.randomUUID(),
        name = "Test Workspace",
        slug = "test-workspace",
        email = "test@example.com",
        initialSetupComplete = true,
        anonymousDataCollection = false,
        sendNewsletter = true,
        displaySetupWizard = false,
        tombstone = false,
        notifications = null,
        firstSyncComplete = true,
        feedbackComplete = true,
        dataplaneGroupId = UUID.randomUUID(),
        webhookOperationConfigs = null,
        notificationSettings = null,
        organizationId = UUID.randomUUID(),
      )
    workspaceRepository.save(workspace)

    workspace.name = "Updated Workspace"
    workspace.dataplaneGroupId = UUID.randomUUID()
    workspaceRepository.update(workspace)

    val updatedWorkspace = workspaceRepository.findById(workspace.id!!)
    assertTrue(updatedWorkspace.isPresent)
    assertEquals("Updated Workspace", updatedWorkspace.get().name)
  }

  @Test
  fun `delete workspace`() {
    val workspace =
      Workspace(
        customerId = UUID.randomUUID(),
        name = "Test Workspace",
        slug = "test-workspace",
        email = "test@example.com",
        initialSetupComplete = true,
        anonymousDataCollection = false,
        sendNewsletter = true,
        displaySetupWizard = false,
        tombstone = false,
        notifications = null,
        firstSyncComplete = true,
        feedbackComplete = true,
        dataplaneGroupId = UUID.randomUUID(),
        webhookOperationConfigs = null,
        notificationSettings = null,
        organizationId = UUID.randomUUID(),
      )
    workspaceRepository.save(workspace)

    workspaceRepository.deleteById(workspace.id!!)

    val deletedWorkspace = workspaceRepository.findById(workspace.id!!)
    assertTrue(deletedWorkspace.isEmpty)
  }

  @Test
  fun `findById returns empty when id does not exist`() {
    val nonExistentId = UUID.randomUUID()

    val result = workspaceRepository.findById(nonExistentId)
    assertTrue(result.isEmpty)
  }
}
