/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.data.repositories.ActorRepository
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.Optional
import java.util.UUID

class AuthorizationDataServiceTest {
  private val workspaceId = UUID.randomUUID()
  private val organizationId = UUID.randomUUID()
  private val workspaces = mockk<WorkspaceService>()
  private val actorRepository = mockk<ActorRepository>()
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
      actorRepository = actorRepository,
      attempts = mockk(),
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

  @Test
  fun `saved actor definition lookup uses only IDs within the authorized workspace`() {
    val actorId = UUID.randomUUID()
    val definitionId = UUID.randomUUID()
    every { actorRepository.findActorDefinitionIdByIdAndWorkspaceId(actorId, workspaceId) } returns Optional.of(definitionId)

    assertEquals(definitionId, service.savedActorDefinitionId(actorId, workspaceId))
    verify(exactly = 0) { actorRepository.findByActorId(any()) }
  }

  @Test
  fun `missing saved definition remains not found`() {
    val actorId = UUID.randomUUID()
    every { actorRepository.findActorDefinitionIdByIdAndWorkspaceId(actorId, workspaceId) } returns Optional.empty()

    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.savedActorDefinitionId(actorId, workspaceId) }
  }

  @Test
  fun `saved definition dependency errors propagate unchanged`() {
    val actorId = UUID.randomUUID()
    val failure = IOException("definition lookup failed")
    every { actorRepository.findActorDefinitionIdByIdAndWorkspaceId(actorId, workspaceId) } throws failure

    assertSame(failure, assertThrows(IOException::class.java) { service.savedActorDefinitionId(actorId, workspaceId) })
  }
}
