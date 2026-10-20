/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.UUID

class AuthorizationDataServiceTest {
  private val workspaceId = UUID.randomUUID()
  private val organizationId = UUID.randomUUID()
  private val workspaces = mockk<WorkspaceService>()
  private val service =
    AuthorizationDataService(
      workspaces,
      mockk(),
      mockk(),
      mockk(),
      mockk(),
      mockk(),
      mockk(),
      mockk(),
      mockk(),
      mockk(),
      mockk(),
      mockk(),
      mockk(),
      mockk(),
      mockk(),
      mockk(),
      mockk(),
      privateLinks = mockk(),
      connectionEvents = mockk(),
    )

  @Test
  fun `workspace and organization IDs come from the saved workspace without loading secrets`() {
    every { workspaces.getStandardWorkspaceNoSecrets(workspaceId, false) } returns
      StandardWorkspace().withWorkspaceId(workspaceId).withOrganizationId(organizationId)
    assertEquals(
      WorkspaceAndOrganization(workspaceId, organizationId),
      assertDoesNotThrow<WorkspaceAndOrganization> {
        service.workspaceAndOrganization(workspaceId)
      },
    )
  }

  @Test
  fun `missing saved workspace keeps not found behavior`() {
    every { workspaces.getStandardWorkspaceNoSecrets(workspaceId, false) } throws
      ConfigNotFoundException(ConfigNotFoundType.STANDARD_WORKSPACE, workspaceId)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.workspaceAndOrganization(workspaceId) }
  }

  @Test
  fun `missing organization cannot become global access`() {
    every { workspaces.getStandardWorkspaceNoSecrets(workspaceId, false) } returns StandardWorkspace().withWorkspaceId(workspaceId)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.workspaceAndOrganization(workspaceId) }
  }

  @Test
  fun `dependency errors propagate without empty workspace and organization IDs`() {
    val failure = IOException("workspace lookup failed")
    every { workspaces.getStandardWorkspaceNoSecrets(workspaceId, false) } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.workspaceAndOrganization(workspaceId) })
  }
}
