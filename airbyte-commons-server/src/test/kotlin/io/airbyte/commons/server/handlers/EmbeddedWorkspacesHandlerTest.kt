/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers
import io.airbyte.api.client.model.generated.WorkspaceCreate
import io.airbyte.api.model.generated.WorkspaceRead
import io.airbyte.data.repositories.WorkspaceRepository
import io.airbyte.data.repositories.entities.Workspace
import io.airbyte.domain.models.OrganizationId
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class EmbeddedWorkspacesHandlerTest {
  val externalUserId = "cool customer"
  val organizationId = UUID.randomUUID()
  val workspaceId = UUID.randomUUID()
  val expectedCreateWorkspaceRequest =
    io.airbyte.api.model.generated
      .WorkspaceCreate()
      .name(externalUserId)
      .organizationId(organizationId)
  val workspaceRead = WorkspaceRead().workspaceId(workspaceId)

  val workspacesHandler = mockk<WorkspacesHandler>()
  val workspaceRepository = mockk<WorkspaceRepository>()
  val existingWorkspace =
    Workspace(
      workspaceId,
      UUID.randomUUID(),
      externalUserId,
      UUID.randomUUID().toString(),
      null,
      true,
      null,
      dataplaneGroupId = UUID.randomUUID(),
      organizationId = organizationId,
    )
  val anotherExistingWorkspace =
    Workspace(
      UUID.randomUUID(),
      UUID.randomUUID(),
      externalUserId,
      UUID.randomUUID().toString(),
      null,
      true,
      null,
      dataplaneGroupId = UUID.randomUUID(),
      organizationId = organizationId,
    )

  lateinit var handler: EmbeddedWorkspacesHandler

  @BeforeEach
  fun setup() {
    handler = EmbeddedWorkspacesHandler(workspacesHandler, workspaceRepository)
  }

  @Test
  fun `test create`() {
    every {
      workspaceRepository.findByNameAndOrganizationId(externalUserId, organizationId)
    } returns emptyList()

    every {
      workspacesHandler.createWorkspace(expectedCreateWorkspaceRequest)
    } returns workspaceRead

    val returnedWorkspaceId = handler.getOrCreate(OrganizationId(organizationId), externalUserId)

    assertEquals(workspaceId, returnedWorkspaceId.value)
  }

  @Test
  fun `test get existing workspace`() {
    every {
      workspaceRepository.findByNameAndOrganizationId(externalUserId, organizationId)
    } returns listOf(existingWorkspace)

    val returnedWorkspaceId = handler.getOrCreate(OrganizationId(organizationId), externalUserId)

    assertEquals(workspaceId, returnedWorkspaceId.value)
  }

  @Test
  fun `test error if multiple workspaces with externalId`() {
    every {
      workspaceRepository.findByNameAndOrganizationId(externalUserId, organizationId)
    } returns listOf(existingWorkspace, anotherExistingWorkspace)

    org.junit.jupiter.api.assertThrows<IllegalStateException> {
      handler.getOrCreate(OrganizationId(organizationId), externalUserId)
    }
  }
}
