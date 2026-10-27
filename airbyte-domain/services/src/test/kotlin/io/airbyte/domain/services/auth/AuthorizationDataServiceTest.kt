/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.repositories.ActorRepository
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
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
  private val users = mockk<UserPersistence>()
  private val actorDefinitions = mockk<ActorDefinitionService>()
  private val connections = mockk<ConnectionService>()
  private val service =
    AuthorizationDataService(
      workspaces,
      mockk(),
      actorDefinitions,
      connections,
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
      users,
      mockk(),
      privateLinks = mockk(),
      connectionEvents = mockk(),
      actorRepository = actorRepository,
      attempts = mockk(),
      entitlements = mockk(),
      featureFlags = mockk(),
    )

  @Test
  fun `historical billing checks both typed connection actors and source definition while allowing tombstoned workspaces`() {
    val connection = UUID(0, 3)
    val source = UUID(0, 4)
    val destination = UUID(0, 5)
    val definition = UUID(0, 6)
    every { connections.getStandardSync(connection) } returns
      StandardSync().withConnectionId(connection).withSourceId(source).withDestinationId(destination)
    every { actorRepository.findSourceWorkspaceId(source) } returns Optional.of(workspaceId)
    every { actorRepository.findDestinationWorkspaceId(destination) } returns Optional.of(workspaceId)
    every { workspaces.getStandardWorkspaceNoSecrets(workspaceId, true) } returns
      StandardWorkspace().withWorkspaceId(workspaceId).withOrganizationId(organizationId).withTombstone(true)
    every { actorRepository.findActorDefinitionIdByIdAndWorkspaceId(source, workspaceId) } returns Optional.of(definition)
    every { actorRepository.findDefinitionTypeById(definition) } returns Optional.of(ActorType.source)
    assertEquals(WorkspaceAndOrganization(workspaceId, organizationId), service.billingConnectionWorkspaceAndOrganization(connection))
    every { actorRepository.findDestinationWorkspaceId(destination) } returns Optional.of(UUID(0, 99))
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.billingConnectionWorkspaceAndOrganization(connection) }
    every { actorRepository.findDestinationWorkspaceId(destination) } returns Optional.of(workspaceId)
    every { actorRepository.findDefinitionTypeById(definition) } returns Optional.of(ActorType.destination)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.billingConnectionWorkspaceAndOrganization(connection) }
  }

  @Test
  fun `global default version metadata requires its saved parent and keeps missing default and lookup errors`() {
    val definition = UUID(0, 30)
    val version = ActorDefinitionVersion().withVersionId(UUID(0, 31)).withActorDefinitionId(definition)
    every { actorDefinitions.getDefaultVersionForActorDefinitionIdOptional(definition) } returns Optional.of(version)
    assertDoesNotThrow { service.requireDefinitionDefaultVersion(definition) }
    version.actorDefinitionId = UUID(0, 99)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.requireDefinitionDefaultVersion(definition) }
    version.actorDefinitionId = definition
    version.versionId = null
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.requireDefinitionDefaultVersion(definition) }
    every { actorDefinitions.getDefaultVersionForActorDefinitionIdOptional(definition) } returns Optional.empty()
    assertThrows(NoSuchElementException::class.java) { service.requireDefinitionDefaultVersion(definition) }
    val failure = IOException("default version lookup unavailable")
    every { actorDefinitions.getDefaultVersionForActorDefinitionIdOptional(definition) } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.requireDefinitionDefaultVersion(definition) })
  }

  @Test
  fun `grant removal kind check permits absent definitions without treating wrong kinds or dependency errors as absence`() {
    val definition = UUID(0, 30)
    every { actorRepository.findDefinitionTypeById(definition) } returns Optional.empty()
    assertDoesNotThrow { service.requireActorDefinitionTypeIfPresent(definition, true) }
    every { actorRepository.findDefinitionTypeById(definition) } returns Optional.of(ActorType.source)
    assertDoesNotThrow { service.requireActorDefinitionTypeIfPresent(definition, true) }
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.requireActorDefinitionTypeIfPresent(definition, false) }
    val failure = IOException("definition kind unavailable")
    every { actorRepository.findDefinitionTypeById(definition) } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.requireActorDefinitionTypeIfPresent(definition, true) })
  }

  @Test
  fun `organization definition availability verifies saved default parent and actual organization grant scope`() {
    val definition = UUID(0, 30)
    val version = ActorDefinitionVersion().withVersionId(UUID(0, 31)).withActorDefinitionId(definition)
    every { actorDefinitions.getDefaultVersionForActorDefinitionIdOptional(definition) } returns Optional.of(version)
    every { actorDefinitions.scopeCanUseDefinition(definition, organizationId, "organization") } returns true
    assertEquals(true, service.organizationCanUseActorDefinition(definition, organizationId))
    every { actorDefinitions.scopeCanUseDefinition(definition, organizationId, "organization") } returns false
    assertEquals(false, service.organizationCanUseActorDefinition(definition, organizationId))
    version.actorDefinitionId = UUID(0, 99)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.organizationCanUseActorDefinition(definition, organizationId) }
    every { actorDefinitions.getDefaultVersionForActorDefinitionIdOptional(definition) } returns Optional.empty()
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.organizationCanUseActorDefinition(definition, organizationId) }
  }

  @Test
  fun `organization creation beneficiary must exist without requiring a linked auth identity`() {
    val id = UUID.randomUUID()
    every { users.getUser(id) } returns
      Optional.of(
        io.airbyte.config
          .User()
          .withUserId(id),
      )
    assertDoesNotThrow { service.requireUser(id) }
    every { users.getUser(id) } returns Optional.empty()
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.requireUser(id) }
    every { users.getUser(id) } returns
      Optional.of(
        io.airbyte.config
          .User()
          .withUserId(UUID.randomUUID()),
      )
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.requireUser(id) }
    val failure = IOException("beneficiary lookup unavailable")
    every { users.getUser(id) } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.requireUser(id) })
    verify(exactly = 0) { users.listAuthUserIdsForUser(any()) }
  }

  @Test
  fun `user auth and exact email selectors use existing saved lookups without linking identities`() {
    val id = UUID.randomUUID()
    every { users.getUserByAuthId("provider|subject") } returns
      Optional.of(
        io.airbyte.config
          .AuthenticatedUser()
          .withUserId(id)
          .withAuthUserId("provider|subject"),
      )
    assertDoesNotThrow { service.requireAuthUserSubject("provider|subject") }
    every { users.getUserByAuthId("provider|subject") } returns Optional.empty()
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.requireAuthUserSubject("provider|subject") }
    every { users.getUserByAuthId("provider|subject") } returns
      Optional.of(
        io.airbyte.config
          .AuthenticatedUser()
          .withUserId(id)
          .withAuthUserId("different"),
      )
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.requireAuthUserSubject("provider|subject") }
    every { users.getUserByEmail("exact@example.com") } returns
      Optional.of(
        io.airbyte.config
          .User()
          .withUserId(id)
          .withEmail("exact@example.com"),
      )
    assertDoesNotThrow { service.requireUserByEmail("exact@example.com") }
    every { users.getUserByEmail("exact@example.com") } returns Optional.empty()
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.requireUserByEmail("exact@example.com") }
    val failure = IOException("user identity unavailable")
    every { users.getUserByAuthId("provider|subject") } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.requireAuthUserSubject("provider|subject") })
    verify(exactly = 0) { users.listAuthUserIdsForUser(any()) }
  }

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
  fun `saved destination ownership uses a destination-specific non-secret lookup`() {
    val id = UUID.randomUUID()
    every { actorRepository.findDestinationWorkspaceId(id) } returns Optional.of(workspaceId)
    every { workspaces.getStandardWorkspaceNoSecrets(workspaceId, false) } returns
      StandardWorkspace().withWorkspaceId(workspaceId).withOrganizationId(organizationId)
    assertEquals(WorkspaceAndOrganization(workspaceId, organizationId), service.savedDestinationWorkspaceAndOrganization(id))
    every { actorRepository.findDestinationWorkspaceId(id) } returns Optional.empty()
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.savedDestinationWorkspaceAndOrganization(id) }
    val failure = IOException("destination ownership unavailable")
    every { actorRepository.findDestinationWorkspaceId(id) } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.savedDestinationWorkspaceAndOrganization(id) })
    verify(exactly = 0) { actorRepository.findByActorId(any()) }
  }

  @Test
  fun `definition checks reject wrong kind and missing definitions`() {
    val id = UUID.randomUUID()
    every { actorRepository.findDefinitionTypeById(id) } returns Optional.of(ActorType.destination)
    assertDoesNotThrow { service.requireActorDefinitionType(id, false) }
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.requireActorDefinitionType(id, true) }
    every { actorRepository.findDefinitionTypeById(id) } returns Optional.empty()
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.requireActorDefinitionType(id, false) }
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
