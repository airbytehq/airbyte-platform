/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.config.persistence.WorkspacePersistence
import io.airbyte.data.services.WorkspaceService
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.Optional
import java.util.UUID

class DefinitionAccessServiceTest {
  private val user = UUID(0, 1)
  private val workspace = UUID(0, 2)
  private val other = UUID(0, 3)
  private val definition = UUID(0, 4)
  private val users = mockk<UserPersistence>()
  private val workspaces = mockk<WorkspacePersistence>()
  private val definitions = mockk<WorkspaceService>()
  private val service = DefinitionAccessService(users, workspaces, definitions)

  @Test
  fun `custom definition access accepts any active readable workspace and does not fetch workspace configurations`() {
    every { workspaces.listActiveWorkspaceIdsByUserId(user) } returns listOf(workspace, other)
    every { definitions.workspaceCanUseCustomDefinition(definition, workspace) } returns false
    every { definitions.workspaceCanUseCustomDefinition(definition, other) } returns true
    assertEquals(true, service.hasAccessibleCustomDefinition(user, definition))
    every { definitions.workspaceCanUseCustomDefinition(definition, other) } returns false
    assertEquals(false, service.hasAccessibleCustomDefinition(user, definition))
    every { workspaces.listActiveWorkspaceIdsByUserId(user) } returns emptyList()
    assertEquals(false, service.hasAccessibleCustomDefinition(user, definition))
    verify(exactly = 0) { workspaces.listActiveWorkspacesByUserId(any(), any()) }
    verify(exactly = 0) { definitions.getWorkspaceWithSecrets(any(), any()) }
    val failure = IOException("custom definition grants unavailable")
    every { workspaces.listActiveWorkspaceIdsByUserId(user) } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.hasAccessibleCustomDefinition(user, definition) })
  }

  @Test
  fun `identity metadata is exact and missing or unlinked users cannot borrow another identity`() {
    every { users.getUserByAuthId("subject") } returns Optional.of(AuthenticatedUser().withUserId(user).withAuthUserId("subject"))
    assertEquals(user, service.currentUserId("subject"))
    every { users.getUserByAuthId("subject") } returns Optional.empty()
    assertThrows(NoSuchElementException::class.java) { service.currentUserId("subject") }
    every { users.getUserByAuthId("subject") } returns Optional.of(AuthenticatedUser().withUserId(user).withAuthUserId("other"))
    assertThrows(IllegalStateException::class.java) { service.currentUserId("subject") }
  }
}
