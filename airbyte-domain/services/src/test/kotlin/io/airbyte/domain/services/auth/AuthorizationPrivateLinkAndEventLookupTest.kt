/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardWorkspace
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.repositories.entities.ConnectionTimelineEvent
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ConnectionTimelineEventService
import io.airbyte.data.services.PrivateLinkService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.shared.ActorWorkspaceOrganizationIds
import io.airbyte.domain.models.PrivateLink
import io.airbyte.domain.models.PrivateLinkServiceConfig
import io.airbyte.domain.models.PrivateLinkServiceType
import io.airbyte.domain.models.PrivateLinkStatus
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.UUID

class AuthorizationPrivateLinkAndEventLookupTest {
  private val id = UUID.randomUUID()
  private val workspace = UUID.randomUUID()
  private val organization = UUID.randomUUID()
  private val connection = UUID.randomUUID()
  private val source = UUID.randomUUID()
  private val destination = UUID.randomUUID()
  private val workspaces = mockk<WorkspaceService>()
  private val privateLinks = mockk<PrivateLinkService>()
  private val events = mockk<ConnectionTimelineEventService>()
  private val connections = mockk<ConnectionService>()
  private val actors = mockk<ActorDefinitionService>()
  private val service =
    AuthorizationDataService(
      workspaces = workspaces,
      permissions = mockk(),
      actors = actors,
      connections = connections,
      operations = mockk(),
      jobs = mockk(),
      organizations = mockk(),
      dataplanes = mockk(),
      dataplaneGroups = mockk(),
      groups = mockk(),
      verifications = mockk(),
      secretStorages = mockk(),
      tags = mockk(),
      projects = mockk(),
      commands = mockk(),
      users = mockk(),
      invitations = mockk(),
      privateLinks = privateLinks,
      connectionEvents = events,
      actorRepository = mockk(),
    )

  private val link =
    PrivateLink(
      id = id,
      workspaceId = workspace,
      dataplaneGroupId = UUID.randomUUID(),
      name = "test link",
      status = PrivateLinkStatus.AVAILABLE,
      serviceType = PrivateLinkServiceType.ENDPOINT,
      serviceConfig = PrivateLinkServiceConfig.Endpoint("service", "us-east-1"),
    )

  init {
    every { privateLinks.getPrivateLink(id) } returns link
    every { workspaces.getStandardWorkspaceNoSecrets(workspace, false) } returns
      StandardWorkspace().withWorkspaceId(workspace).withOrganizationId(organization)
    every { events.getEvent(id) } returns ConnectionTimelineEvent(id = id, connectionId = connection, eventType = "SYNC_SUCCEEDED")
    every { connections.getStandardSync(connection) } returns
      StandardSync().withConnectionId(connection).withSourceId(source).withDestinationId(destination)
    for (actor in listOf(source, destination)) {
      every { actors.getIdsForActors(listOf(actor)) } returns listOf(ActorWorkspaceOrganizationIds(actor, workspace, organization))
    }
  }

  @Test
  fun `private link resolves saved workspace and reads changed ownership on every call`() {
    assertEquals(WorkspaceAndOrganization(workspace, organization), service.privateLinkWorkspaceAndOrganization(id))

    val movedWorkspace = UUID.randomUUID()
    val movedOrganization = UUID.randomUUID()
    every { privateLinks.getPrivateLink(id) } returns link.copy(workspaceId = movedWorkspace)
    every { workspaces.getStandardWorkspaceNoSecrets(movedWorkspace, false) } returns
      StandardWorkspace().withWorkspaceId(movedWorkspace).withOrganizationId(movedOrganization)
    assertEquals(WorkspaceAndOrganization(movedWorkspace, movedOrganization), service.privateLinkWorkspaceAndOrganization(id))

    every { workspaces.getStandardWorkspaceNoSecrets(movedWorkspace, false) } returns
      StandardWorkspace().withWorkspaceId(movedWorkspace).withOrganizationId(organization)
    assertEquals(WorkspaceAndOrganization(movedWorkspace, organization), service.privateLinkWorkspaceAndOrganization(id))
  }

  @Test
  fun `missing private link becomes resource not found`() {
    every { privateLinks.getPrivateLink(id) } throws NoSuchElementException("private link missing")
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.privateLinkWorkspaceAndOrganization(id) }
  }

  @Test
  fun `private link with absent or wrong saved identity fails before workspace lookup`() {
    for (savedId in listOf(null, UUID.randomUUID())) {
      every { privateLinks.getPrivateLink(id) } returns link.copy(id = savedId)
      assertThrows(AuthorizationResourceNotFoundException::class.java) { service.privateLinkWorkspaceAndOrganization(id) }
    }
    verify(exactly = 0) { workspaces.getStandardWorkspaceNoSecrets(any(), any()) }
  }

  @Test
  fun `private link missing workspace or organization stays not found`() {
    every { workspaces.getStandardWorkspaceNoSecrets(workspace, false) } throws
      ConfigNotFoundException(ConfigNotFoundType.STANDARD_WORKSPACE, workspace)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.privateLinkWorkspaceAndOrganization(id) }

    every { workspaces.getStandardWorkspaceNoSecrets(workspace, false) } returns StandardWorkspace().withWorkspaceId(workspace)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.privateLinkWorkspaceAndOrganization(id) }
  }

  @Test
  fun `private link and workspace dependency errors propagate unchanged`() {
    val linkFailure = IOException("private links unavailable")
    every { privateLinks.getPrivateLink(id) } throws linkFailure
    assertSame(linkFailure, assertThrows(IOException::class.java) { service.privateLinkWorkspaceAndOrganization(id) })

    every { privateLinks.getPrivateLink(id) } returns link
    val workspaceFailure = IOException("workspaces unavailable")
    every { workspaces.getStandardWorkspaceNoSecrets(workspace, false) } throws workspaceFailure
    assertSame(workspaceFailure, assertThrows(IOException::class.java) { service.privateLinkWorkspaceAndOrganization(id) })
  }

  @Test
  fun `private link missing exception translation does not hide downstream bugs`() {
    val failure = NoSuchElementException("unexpected workspace failure")
    every { workspaces.getStandardWorkspaceNoSecrets(workspace, false) } throws failure
    assertSame(failure, assertThrows(NoSuchElementException::class.java) { service.privateLinkWorkspaceAndOrganization(id) })
  }

  @Test
  fun `connection event resolves its saved parent and reads changed ownership on every call`() {
    assertEquals(WorkspaceAndOrganization(workspace, organization), service.connectionEventWorkspaceAndOrganization(id, connection))

    val movedWorkspace = UUID.randomUUID()
    val movedOrganization = UUID.randomUUID()
    for (actor in listOf(source, destination)) {
      every { actors.getIdsForActors(listOf(actor)) } returns listOf(ActorWorkspaceOrganizationIds(actor, movedWorkspace, movedOrganization))
    }
    assertEquals(WorkspaceAndOrganization(movedWorkspace, movedOrganization), service.connectionEventWorkspaceAndOrganization(id, connection))

    val movedConnection = UUID.randomUUID()
    every { events.getEvent(id) } returns ConnectionTimelineEvent(id = id, connectionId = movedConnection, eventType = "SYNC_SUCCEEDED")
    every { connections.getStandardSync(movedConnection) } returns
      StandardSync().withConnectionId(movedConnection).withSourceId(source).withDestinationId(destination)
    assertEquals(WorkspaceAndOrganization(movedWorkspace, movedOrganization), service.connectionEventWorkspaceAndOrganization(id, movedConnection))
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.connectionEventWorkspaceAndOrganization(id, connection) }
  }

  @Test
  fun `missing connection event becomes resource not found`() {
    every { events.getEvent(id) } throws NoSuchElementException("event missing")
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.connectionEventWorkspaceAndOrganization(id, connection) }
  }

  @Test
  fun `connection event with absent or wrong saved identity fails before parent lookup`() {
    for (savedId in listOf(null, UUID.randomUUID())) {
      every { events.getEvent(id) } returns ConnectionTimelineEvent(id = savedId, connectionId = connection, eventType = "SYNC_SUCCEEDED")
      assertThrows(AuthorizationResourceNotFoundException::class.java) { service.connectionEventWorkspaceAndOrganization(id, connection) }
    }
    verify(exactly = 0) { connections.getStandardSync(any()) }
    verify(exactly = 0) { actors.getIdsForActors(any()) }
  }

  @Test
  fun `connection event from another connection in the same workspace fails before tenant lookup`() {
    val otherConnection = UUID.randomUUID()
    every { connections.getStandardSync(otherConnection) } returns
      StandardSync().withConnectionId(otherConnection).withSourceId(source).withDestinationId(destination)
    every { events.getEvent(id) } returns ConnectionTimelineEvent(id = id, connectionId = otherConnection, eventType = "SYNC_SUCCEEDED")

    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.connectionEventWorkspaceAndOrganization(id, connection) }
    verify(exactly = 0) { connections.getStandardSync(any()) }
    verify(exactly = 0) { actors.getIdsForActors(any()) }
  }

  @Test
  fun `connection event missing or wrong saved connection stays not found`() {
    every { connections.getStandardSync(connection) } throws ConfigNotFoundException(ConfigNotFoundType.STANDARD_SYNC, connection)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.connectionEventWorkspaceAndOrganization(id, connection) }

    every { connections.getStandardSync(connection) } returns
      StandardSync().withConnectionId(UUID.randomUUID()).withSourceId(source).withDestinationId(destination)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.connectionEventWorkspaceAndOrganization(id, connection) }
  }

  @Test
  fun `connection event missing actor workspace or organization stays not found`() {
    // The actor projection joins workspace, so a missing actor or workspace returns no row.
    every { actors.getIdsForActors(listOf(source)) } returns emptyList()
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.connectionEventWorkspaceAndOrganization(id, connection) }

    every { actors.getIdsForActors(listOf(source)) } returns listOf(ActorWorkspaceOrganizationIds(source, workspace, null))
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.connectionEventWorkspaceAndOrganization(id, connection) }
  }

  @Test
  fun `connection event contradictory parent ownership fails closed`() {
    every { actors.getIdsForActors(listOf(destination)) } returns
      listOf(ActorWorkspaceOrganizationIds(destination, UUID.randomUUID(), organization))
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.connectionEventWorkspaceAndOrganization(id, connection) }
  }

  @Test
  fun `connection event parent and actor dependency errors propagate unchanged`() {
    val eventFailure = IOException("events unavailable")
    every { events.getEvent(id) } throws eventFailure
    assertSame(eventFailure, assertThrows(IOException::class.java) { service.connectionEventWorkspaceAndOrganization(id, connection) })

    every { events.getEvent(id) } returns ConnectionTimelineEvent(id = id, connectionId = connection, eventType = "SYNC_SUCCEEDED")
    val connectionFailure = IOException("connections unavailable")
    every { connections.getStandardSync(connection) } throws connectionFailure
    assertSame(connectionFailure, assertThrows(IOException::class.java) { service.connectionEventWorkspaceAndOrganization(id, connection) })

    every { connections.getStandardSync(connection) } returns
      StandardSync().withConnectionId(connection).withSourceId(source).withDestinationId(destination)
    val actorFailure = IOException("actors unavailable")
    every { actors.getIdsForActors(listOf(destination)) } throws actorFailure
    assertSame(actorFailure, assertThrows(IOException::class.java) { service.connectionEventWorkspaceAndOrganization(id, connection) })
  }

  @Test
  fun `connection event missing exception translation does not hide downstream bugs`() {
    val failure = NoSuchElementException("unexpected connection failure")
    every { connections.getStandardSync(connection) } throws failure
    assertSame(failure, assertThrows(NoSuchElementException::class.java) { service.connectionEventWorkspaceAndOrganization(id, connection) })
  }
}
