/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.ConnectorBuilderProject
import io.airbyte.config.Dataplane
import io.airbyte.config.DataplaneGroup
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.Organization
import io.airbyte.config.Permission
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSyncOperation
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.Tag
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.repositories.AuthorizationCommandRepository
import io.airbyte.data.repositories.GroupRepository
import io.airbyte.data.repositories.OrganizationDomainVerificationRepository
import io.airbyte.data.repositories.entities.AuthorizationCommand
import io.airbyte.data.repositories.entities.Group
import io.airbyte.data.repositories.entities.OrganizationDomainVerification
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ConnectorBuilderService
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneService
import io.airbyte.data.services.JobService
import io.airbyte.data.services.OperationService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.SecretStorageService
import io.airbyte.data.services.TagService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.shared.ActorWorkspaceOrganizationIds
import io.airbyte.db.instance.configs.jooq.generated.enums.DomainVerificationMethod
import io.airbyte.db.instance.configs.jooq.generated.enums.DomainVerificationStatus
import io.airbyte.domain.models.SecretStorage
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.SecretStorageScopeType
import io.airbyte.domain.models.SecretStorageType
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.AuthorizationTarget
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.Optional
import java.util.UUID

class AuthorizationResourceLookupTest {
  private val workspace = UUID.randomUUID()
  private val org = UUID.randomUUID()
  private val id = UUID.randomUUID()
  private val source = UUID.randomUUID()
  private val destination = UUID.randomUUID()
  private val pair = WorkspaceAndOrganization(workspace, org)
  private val workspaces = mockk<WorkspaceService>()
  private val actors = mockk<ActorDefinitionService>()
  private val connections = mockk<ConnectionService>()
  private val operations = mockk<OperationService>()
  private val jobs = mockk<JobService>()
  private val permissions = mockk<PermissionService>()
  private val organizations = mockk<OrganizationService>()
  private val dataplanes = mockk<DataplaneService>()
  private val groups = mockk<DataplaneGroupService>()
  private val userGroups = mockk<GroupRepository>()
  private val verifications = mockk<OrganizationDomainVerificationRepository>()
  private val secretStorages = mockk<SecretStorageService>()
  private val tags = mockk<TagService>()
  private val projects = mockk<ConnectorBuilderService>()
  private val commands = mockk<AuthorizationCommandRepository>()
  private val users = mockk<UserPersistence>()
  private val service =
    AuthorizationDataService(
      workspaces,
      permissions,
      actors,
      connections,
      operations,
      jobs,
      organizations,
      dataplanes,
      groups,
      userGroups,
      verifications,
      secretStorages,
      tags,
      projects,
      commands,
      users,
      mockk(),
      privateLinks = mockk(),
      connectionEvents = mockk(),
      actorRepository = mockk(),
      attempts = mockk(),
    )

  init {
    every { workspaces.getStandardWorkspaceNoSecrets(workspace, false) } returns
      StandardWorkspace().withWorkspaceId(workspace).withOrganizationId(org)
    every { organizations.getOrganization(org) } returns Optional.of(Organization().withOrganizationId(org))
    for (actor in listOf(id, source, destination)) {
      every { actors.getIdsForActors(listOf(actor)) } returns listOf(ActorWorkspaceOrganizationIds(actor, workspace, org))
    }
    every { connections.getStandardSync(id) } returns StandardSync().withConnectionId(id).withSourceId(source).withDestinationId(destination)
  }

  @Test
  fun `actor source and destination read non-secret saved ownership on every call`() {
    for (lookup in listOf(
      service::actorWorkspaceAndOrganization,
      service::sourceWorkspaceAndOrganization,
      service::destinationWorkspaceAndOrganization,
    )) {
      assertEquals(pair, lookup(id))
    }
    val moved = WorkspaceAndOrganization(UUID.randomUUID(), UUID.randomUUID())
    every { actors.getIdsForActors(listOf(id)) } returns listOf(ActorWorkspaceOrganizationIds(id, moved.workspaceId, moved.organizationId))
    assertEquals(moved, service.sourceWorkspaceAndOrganization(id))
  }

  @Test
  fun `actor absent missing organization ambiguous or different identity fails closed`() {
    for (saved in listOf(
      emptyList(),
      listOf(ActorWorkspaceOrganizationIds(id, workspace, null)),
      listOf(ActorWorkspaceOrganizationIds(source, workspace, org)),
      listOf(ActorWorkspaceOrganizationIds(id, workspace, org), ActorWorkspaceOrganizationIds(id, workspace, org)),
    )) {
      every { actors.getIdsForActors(listOf(id)) } returns saved
      assertThrows(AuthorizationResourceNotFoundException::class.java) { service.actorWorkspaceAndOrganization(id) }
    }
  }

  @Test
  fun `connections use both saved actors and reject contradictory or missing ownership`() {
    assertEquals(pair, service.connectionWorkspaceAndOrganization(id))
    every { actors.getIdsForActors(listOf(destination)) } returns listOf(ActorWorkspaceOrganizationIds(destination, UUID.randomUUID(), org))
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.connectionWorkspaceAndOrganization(id) }
    every { actors.getIdsForActors(listOf(destination)) } returns emptyList()
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.connectionWorkspaceAndOrganization(id) }
  }

  @Test
  fun `connection missing actor IDs and wrong returned resource fail closed`() {
    every { connections.getStandardSync(id) } returns StandardSync().withConnectionId(id).withSourceId(source)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.connectionWorkspaceAndOrganization(id) }
    every { connections.getStandardSync(id) } returns StandardSync().withConnectionId(source).withSourceId(source).withDestinationId(destination)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.connectionWorkspaceAndOrganization(id) }
  }

  @Test
  fun `operation resolves its saved workspace and missing relationship is not found`() {
    every { operations.getStandardSyncOperation(id) } returns StandardSyncOperation().withOperationId(id).withWorkspaceId(workspace)
    assertEquals(pair, service.operationWorkspaceAndOrganization(id))
    every { operations.getStandardSyncOperation(id) } returns StandardSyncOperation().withOperationId(id)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.operationWorkspaceAndOrganization(id) }
  }

  @Test
  fun `all replication job types resolve saved connection scope`() {
    for (type in Job.REPLICATION_TYPES) {
      every { jobs.findById(0L) } returns job(type, id.toString())
      assertEquals(pair, service.jobWorkspaceAndOrganization(0L))
    }
  }

  @Test
  fun `missing unsupported and malformed job scope fail closed`() {
    every { jobs.findById(0L) } returns null
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.jobWorkspaceAndOrganization(0L) }
    every { jobs.findById(0L) } returns job(JobConfig.ConfigType.CHECK_CONNECTION_SOURCE, id.toString())
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.jobWorkspaceAndOrganization(0L) }
    every { jobs.findById(0L) } returns job(JobConfig.ConfigType.SYNC, "invalid")
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.jobWorkspaceAndOrganization(0L) }
  }

  @Test
  fun `permission targets distinguish workspace organization and explicit global grants`() {
    every { permissions.getPermission(id) } returns
      Permission().withPermissionId(id).withPermissionType(Permission.PermissionType.WORKSPACE_READER).withWorkspaceId(workspace)
    assertEquals(AuthorizationTarget.Workspace(pair), service.permissionTarget(id))
    every { permissions.getPermission(id) } returns
      Permission().withPermissionId(id).withPermissionType(Permission.PermissionType.ORGANIZATION_READER).withOrganizationId(org)
    assertEquals(AuthorizationTarget.Organization(org), service.permissionTarget(id))
    every { permissions.getPermission(id) } returns Permission().withPermissionId(id).withPermissionType(Permission.PermissionType.INSTANCE_ADMIN)
    assertEquals(AuthorizationTarget.Global, service.permissionTarget(id))
  }

  @Test
  fun `permission absent scope contradictory scope or wrong id never becomes global`() {
    for (permission in listOf(
      Permission().withPermissionId(id).withPermissionType(Permission.PermissionType.WORKSPACE_READER),
      Permission().withPermissionId(id).withWorkspaceId(workspace).withOrganizationId(org),
      Permission().withPermissionId(source).withPermissionType(Permission.PermissionType.INSTANCE_ADMIN),
    )) {
      every { permissions.getPermission(id) } returns permission
      assertThrows(AuthorizationResourceNotFoundException::class.java) { service.permissionTarget(id) }
    }
  }

  @Test
  fun `organization missing or wrong identity is not found`() {
    assertEquals(org, service.organization(org))
    every { organizations.getOrganization(org) } returns Optional.empty()
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.organization(org) }
    every { organizations.getOrganization(org) } returns Optional.of(Organization().withOrganizationId(id))
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.organization(org) }
  }

  @Test
  fun `dataplane and region group resolve their actual saved organization`() {
    every { dataplanes.getDataplane(id) } returns Dataplane().withId(id).withDataplaneGroupId(source)
    every { groups.getDataplaneGroup(source) } returns DataplaneGroup().withId(source).withOrganizationId(org)
    assertEquals(org, service.dataplaneOrganization(id))
    assertEquals(org, service.dataplaneGroupOrganization(source))
    every { groups.getDataplaneGroup(source) } returns DataplaneGroup().withId(source)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.dataplaneOrganization(id) }
  }

  @Test
  fun `group and domain verification resolve saved owners and reject missing records`() {
    every { userGroups.findById(id) } returns Optional.of(Group(id, "group", organizationId = org))
    assertEquals(org, service.groupOrganization(id))
    every { userGroups.findById(id) } returns Optional.empty()
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.groupOrganization(id) }
    val verification = OrganizationDomainVerification(id, org, "example.com", DomainVerificationMethod.dns_txt, DomainVerificationStatus.pending)
    every { verifications.findById(id) } returns Optional.of(verification)
    assertEquals(org, service.domainVerificationOrganization(id))
    every { verifications.findById(id) } returns Optional.of(verification.copy(tombstone = true))
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.domainVerificationOrganization(id) }
    every { verifications.findById(id) } returns Optional.empty()
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.domainVerificationOrganization(id) }
  }

  @Test
  fun `secret storage uses its saved scope without loading secrets`() {
    for (scope in SecretStorageScopeType.entries) {
      val storage =
        SecretStorage(
          SecretStorageId(id),
          scope,
          if (scope ==
            SecretStorageScopeType.WORKSPACE
          ) {
            workspace
          } else {
            org
          },
          "storage",
          SecretStorageType.LOCAL_TESTING,
          false,
          createdBy = source,
          updatedBy = source,
          createdAt = null,
          updatedAt = null,
        )
      every { secretStorages.findById(SecretStorageId(id)) } returns storage
      assertEquals(
        if (scope ==
          SecretStorageScopeType.WORKSPACE
        ) {
          AuthorizationTarget.Workspace(pair)
        } else {
          AuthorizationTarget.Organization(org)
        },
        service.secretStorageTarget(id),
      )
    }
    every { secretStorages.findById(SecretStorageId(id)) } returns null
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.secretStorageTarget(id) }
  }

  @Test
  fun `tag and builder project use saved workspace`() {
    every { tags.getTagById(id) } returns Tag().withTagId(id).withWorkspaceId(workspace)
    every { projects.getConnectorBuilderProject(id, false) } returns ConnectorBuilderProject().withBuilderProjectId(id).withWorkspaceId(workspace)
    assertEquals(pair, service.tagWorkspaceAndOrganization(id))
    assertEquals(pair, service.builderProjectWorkspaceAndOrganization(id))
  }

  @Test
  fun `command queries carry explicit organization and verify saved workspace`() {
    every { commands.findByIdAndOrganizationId("command", org) } returns Optional.of(AuthorizationCommand("command", workspace, org))
    assertEquals(pair, service.commandWorkspaceAndOrganization("command", org))
    every { workspaces.getStandardWorkspaceNoSecrets(workspace, false) } returns StandardWorkspace().withWorkspaceId(workspace).withOrganizationId(id)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.commandWorkspaceAndOrganization("command", org) }
  }

  @Test
  fun `user IDs resolve every linked authentication subject and reject missing identity`() {
    every { users.listAuthUserIdsForUser(id) } returns listOf("primary", "linked")
    assertEquals(setOf("primary", "linked"), service.userAuthSubjects(id))
    every { users.listAuthUserIdsForUser(id) } returns emptyList()
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.userAuthSubjects(id) }
    every { users.listAuthUserIdsForUser(id) } returns listOf("primary", "")
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.userAuthSubjects(id) }
  }

  @Test
  fun `missing saved metadata never produces an authorization target`() {
    every { operations.getStandardSyncOperation(id) } throws ConfigNotFoundException(ConfigNotFoundType.STANDARD_SYNC_OPERATION, id)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.operationWorkspaceAndOrganization(id) }
    every { permissions.getPermission(id) } throws ConfigNotFoundException(ConfigNotFoundType.PERMISSION, id)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.permissionTarget(id) }
    every { dataplanes.getDataplane(id) } throws ConfigNotFoundException("Dataplane", id.toString())
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.dataplaneOrganization(id) }
    every { groups.getDataplaneGroup(id) } throws ConfigNotFoundException(ConfigNotFoundType.DATAPLANE_GROUP, id)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.dataplaneGroupOrganization(id) }
    every { tags.getTagById(id) } throws ConfigNotFoundException(ConfigNotFoundType.TAG, id)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.tagWorkspaceAndOrganization(id) }
    every { projects.getConnectorBuilderProject(id, false) } throws ConfigNotFoundException(ConfigNotFoundType.CONNECTOR_BUILDER_PROJECT, id)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.builderProjectWorkspaceAndOrganization(id) }
    every { commands.findByIdAndOrganizationId("missing", org) } returns Optional.empty()
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.commandWorkspaceAndOrganization("missing", org) }
  }

  @Test
  fun `wrong saved identity or incomplete metadata cannot authorize another resource`() {
    every { dataplanes.getDataplane(id) } returns Dataplane().withId(id)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.dataplaneOrganization(id) }
    every { groups.getDataplaneGroup(id) } returns DataplaneGroup().withId(source).withOrganizationId(org)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.dataplaneGroupOrganization(id) }
    every { tags.getTagById(id) } returns Tag().withTagId(id)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.tagWorkspaceAndOrganization(id) }
    every { projects.getConnectorBuilderProject(id, false) } returns ConnectorBuilderProject().withBuilderProjectId(id)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.builderProjectWorkspaceAndOrganization(id) }
    every { commands.findByIdAndOrganizationId("command", org) } returns Optional.of(AuthorizationCommand("wrong", workspace, org))
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.commandWorkspaceAndOrganization("command", org) }
  }

  @Test
  fun `resource not-found errors translate but dependency errors propagate`() {
    every { connections.getStandardSync(id) } throws ConfigNotFoundException(ConfigNotFoundType.STANDARD_SYNC, id)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.connectionWorkspaceAndOrganization(id) }
    val failure = IOException("metadata unavailable")
    every { actors.getIdsForActors(listOf(id)) } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.sourceWorkspaceAndOrganization(id) })
    every { permissions.getPermission(id) } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.permissionTarget(id) })
  }

  private fun job(
    type: JobConfig.ConfigType,
    scope: String,
  ): Job =
    mockk<Job>().also {
      every { it.id } returns 0L
      every { it.configType } returns type
      every { it.scope } returns scope
    }
}
