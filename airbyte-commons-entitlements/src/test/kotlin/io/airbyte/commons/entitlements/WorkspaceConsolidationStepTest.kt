/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.config.DestinationConnection
import io.airbyte.config.Permission
import io.airbyte.config.ScopeType
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSyncOperation
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.Tag
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.config.persistence.WorkspacePersistence
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ConnectorBuilderService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OAuthService
import io.airbyte.data.services.OperationService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.TagService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.models.OrganizationId
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID

class WorkspaceConsolidationStepTest {
  private val workspacePersistence = mockk<WorkspacePersistence>()
  private val workspaceService = mockk<WorkspaceService>(relaxed = true)
  private val sourceService = mockk<SourceService>(relaxed = true)
  private val destinationService = mockk<DestinationService>(relaxed = true)
  private val connectionService = mockk<ConnectionService>(relaxed = true)
  private val operationService = mockk<OperationService>(relaxed = true)
  private val tagService = mockk<TagService>(relaxed = true)
  private val permissionService = mockk<PermissionService>(relaxed = true)
  private val actorDefinitionService = mockk<ActorDefinitionService>(relaxed = true)
  private val oAuthService = mockk<OAuthService>(relaxed = true)
  private val connectorBuilderService = mockk<ConnectorBuilderService>(relaxed = true)
  private val userPersistence = mockk<UserPersistence>(relaxed = true)
  private val step =
    WorkspaceConsolidationStep(
      workspacePersistence,
      workspaceService,
      sourceService,
      destinationService,
      connectionService,
      operationService,
      tagService,
      permissionService,
      actorDefinitionService,
      oAuthService,
      connectorBuilderService,
      userPersistence,
    )

  private val orgId = OrganizationId(UUID.randomUUID())
  private val dataplaneGroupId = UUID.randomUUID()
  private val kept = workspace("first", createdAt = 1L)
  private val extra = workspace("second", createdAt = 2L)

  private val sourceId = UUID.randomUUID()
  private val destinationId = UUID.randomUUID()
  private val operationId = UUID.randomUUID()
  private val connectionId = UUID.randomUUID()
  private val oldTag =
    Tag()
      .withTagId(UUID.randomUUID())
      .withWorkspaceId(extra.workspaceId)
      .withName("prod")
      .withColor("#ff0000")
  private val connection =
    StandardSync()
      .withConnectionId(connectionId)
      .withSourceId(sourceId)
      .withDestinationId(destinationId)
      .withOperationIds(listOf(operationId))
      .withTags(listOf(oldTag))

  @BeforeEach
  fun setup() {
    every { workspacePersistence.listWorkspacesByOrganizationId(orgId.value, false, Optional.empty()) } returns listOf(extra, kept)
    every { workspacePersistence.getDefaultWorkspaceForOrganization(orgId.value) } returns kept
    every { workspaceService.getStandardWorkspaceNoSecrets(extra.workspaceId, true) } returns
      workspace("second", createdAt = 2L, id = extra.workspaceId)

    every { connectionService.listWorkspaceStandardSyncs(extra.workspaceId, false) } returns listOf(connection)
    every { connectionService.getStandardSync(connectionId) } returns connection
    every { sourceService.listWorkspaceSourceConnection(extra.workspaceId) } returns
      listOf(SourceConnection().withSourceId(sourceId).withWorkspaceId(extra.workspaceId))
    every { destinationService.listWorkspaceDestinationConnection(extra.workspaceId) } returns
      listOf(DestinationConnection().withDestinationId(destinationId).withWorkspaceId(extra.workspaceId))
    every { sourceService.listGrantedSourceDefinitions(extra.workspaceId, true) } returns mutableListOf()
    every { destinationService.listGrantedDestinationDefinitions(extra.workspaceId, true) } returns mutableListOf()
    every { operationService.getStandardSyncOperation(operationId) } returns
      StandardSyncOperation().withOperationId(operationId).withWorkspaceId(extra.workspaceId)
    every { tagService.getTagsByWorkspaceIds(listOf(extra.workspaceId)) } returns listOf(oldTag)
    every { tagService.getTagsByWorkspaceIds(listOf(kept.workspaceId)) } returns emptyList()
    every { tagService.createTag(any()) } answers { firstArg() }
    every { permissionService.getPermissionsByWorkspaceId(extra.workspaceId) } returns emptyList()
    every { permissionService.getPermissionsByWorkspaceId(kept.workspaceId) } returns emptyList()
  }

  @Test
  fun `does nothing when the organization is within its limit`() {
    every { workspacePersistence.listWorkspacesByOrganizationId(orgId.value, false, Optional.empty()) } returns listOf(kept)

    val result = step.run(orgId, 1L)

    assertEquals(PlanLimitStepResult.NotNeeded, result)
    verify(exactly = 0) { workspacePersistence.getDefaultWorkspaceForOrganization(any()) }
    verify(exactly = 0) { workspaceService.writeStandardWorkspaceNoSecrets(any()) }
  }

  @Test
  fun `moves everything from the newer workspace into the oldest one and tombstones it`() {
    val customDefinitionId = UUID.randomUUID()
    every { sourceService.listGrantedSourceDefinitions(extra.workspaceId, true) } returns
      mutableListOf(StandardSourceDefinition().withSourceDefinitionId(customDefinitionId))
    every { actorDefinitionService.actorDefinitionWorkspaceGrantExists(customDefinitionId, kept.workspaceId, ScopeType.WORKSPACE) } returns false

    val userOnlyOnExtra = UUID.randomUUID()
    val userOnBoth = UUID.randomUUID()
    val movedPermission = permission(userOnlyOnExtra, extra.workspaceId)
    val duplicatePermission = permission(userOnBoth, extra.workspaceId)
    every { permissionService.getPermissionsByWorkspaceId(extra.workspaceId) } returns listOf(movedPermission, duplicatePermission)
    every { permissionService.getPermissionsByWorkspaceId(kept.workspaceId) } returns listOf(permission(userOnBoth, kept.workspaceId))

    val result = step.run(orgId, 1L)

    assertInstanceOf(PlanLimitStepResult.Completed::class.java, result)
    assertTrue((result as PlanLimitStepResult.Completed).summary.contains("merged 1 workspace(s) into ${kept.workspaceId}"))
    assertTrue(result.summary.contains("moved 1 connection(s), 0 changed region"))

    verify { actorDefinitionService.writeActorDefinitionWorkspaceGrant(customDefinitionId, kept.workspaceId, ScopeType.WORKSPACE) }
    verify { actorDefinitionService.deleteActorDefinitionWorkspaceGrant(customDefinitionId, extra.workspaceId, ScopeType.WORKSPACE) }
    verify { oAuthService.reassignWorkspaceOAuthParams(extra.workspaceId, kept.workspaceId) }
    verify { sourceService.writeSourceConnectionNoSecrets(match { it.sourceId == sourceId && it.workspaceId == kept.workspaceId }) }
    verify {
      destinationService.writeDestinationConnectionNoSecrets(
        match { it.destinationId == destinationId && it.workspaceId == kept.workspaceId },
      )
    }
    verify { operationService.writeStandardSyncOperation(match { it.operationId == operationId && it.workspaceId == kept.workspaceId }) }
    verify { tagService.createTag(match { it.workspaceId == kept.workspaceId && it.name == "prod" && it.color == "#ff0000" }) }
    verify {
      connectionService.writeStandardSync(
        match { it.connectionId == connectionId && it.tags.single().workspaceId == kept.workspaceId && it.tags.single().name == "prod" },
      )
    }
    verify { tagService.deleteTag(oldTag.tagId) }
    verify { connectorBuilderService.reassignWorkspaceBuilderProjects(extra.workspaceId, kept.workspaceId) }
    verify { permissionService.deletePermissions(listOf(duplicatePermission.permissionId)) }
    verify { permissionService.updatePermissions(match { it.single().userId == userOnlyOnExtra && it.single().workspaceId == kept.workspaceId }) }
    verify { userPersistence.reassignDefaultWorkspace(extra.workspaceId, kept.workspaceId) }
    verify { workspaceService.writeStandardWorkspaceNoSecrets(match { it.workspaceId == extra.workspaceId && it.tombstone == true }) }
    verify(exactly = 0) { workspaceService.writeStandardWorkspaceNoSecrets(match { it.workspaceId == kept.workspaceId }) }
  }

  @Test
  fun `moves connections that change region and reports them`() {
    val otherRegion = workspace("second", createdAt = 2L, id = extra.workspaceId, dataplaneGroup = UUID.randomUUID())
    every { workspacePersistence.listWorkspacesByOrganizationId(orgId.value, false, Optional.empty()) } returns listOf(otherRegion, kept)

    val result = step.run(orgId, 1L)

    assertTrue((result as PlanLimitStepResult.Completed).summary.contains("moved 1 connection(s), 1 changed region"))
    verify { sourceService.writeSourceConnectionNoSecrets(match { it.workspaceId == kept.workspaceId }) }
    verify { workspaceService.writeStandardWorkspaceNoSecrets(match { it.workspaceId == extra.workspaceId && it.tombstone == true }) }
  }

  @Test
  fun `drops a tag when the kept workspace is at its tag limit`() {
    every { tagService.createTag(any()) } throws IllegalStateException("Maximum 100 tags can be created in a workspace")

    step.run(orgId, 1L)

    verify { connectionService.writeStandardSync(match { it.connectionId == connectionId && it.tags.isEmpty() }) }
    verify { tagService.deleteTag(oldTag.tagId) }
  }

  @Test
  fun `reuses a same-named tag that already exists in the kept workspace`() {
    val existing =
      Tag()
        .withTagId(UUID.randomUUID())
        .withWorkspaceId(kept.workspaceId)
        .withName("prod")
        .withColor("#00ff00")
    every { tagService.getTagsByWorkspaceIds(listOf(kept.workspaceId)) } returns listOf(existing)

    step.run(orgId, 1L)

    verify(exactly = 0) { tagService.createTag(any()) }
    verify { connectionService.writeStandardSync(match { it.tags.single().tagId == existing.tagId }) }
  }

  @Test
  fun `leaves the workspace in place when a move fails`() {
    every { sourceService.writeSourceConnectionNoSecrets(any()) } throws RuntimeException("db unavailable")

    assertThrows(RuntimeException::class.java) { step.run(orgId, 1L) }

    verify(exactly = 0) { workspaceService.writeStandardWorkspaceNoSecrets(any()) }
    verify(exactly = 0) { userPersistence.reassignDefaultWorkspace(any(), any()) }
  }

  @Test
  fun `keeps the oldest workspaces when the limit is above one`() {
    val newest = workspace("third", createdAt = 3L)
    every { workspacePersistence.listWorkspacesByOrganizationId(orgId.value, false, Optional.empty()) } returns listOf(kept, newest, extra)
    every { workspaceService.getStandardWorkspaceNoSecrets(newest.workspaceId, true) } returns newest
    every { connectionService.listWorkspaceStandardSyncs(newest.workspaceId, false) } returns emptyList()
    every { sourceService.listWorkspaceSourceConnection(newest.workspaceId) } returns emptyList()
    every { destinationService.listWorkspaceDestinationConnection(newest.workspaceId) } returns emptyList()
    every { sourceService.listGrantedSourceDefinitions(newest.workspaceId, true) } returns mutableListOf()
    every { destinationService.listGrantedDestinationDefinitions(newest.workspaceId, true) } returns mutableListOf()
    every { tagService.getTagsByWorkspaceIds(listOf(newest.workspaceId)) } returns emptyList()
    every { permissionService.getPermissionsByWorkspaceId(newest.workspaceId) } returns emptyList()

    val result = step.run(orgId, 2L)

    assertTrue((result as PlanLimitStepResult.Completed).summary.contains("merged 1 workspace(s)"))
    verify { workspaceService.writeStandardWorkspaceNoSecrets(match { it.workspaceId == newest.workspaceId && it.tombstone == true }) }
    verify(exactly = 0) { workspaceService.writeStandardWorkspaceNoSecrets(match { it.workspaceId == extra.workspaceId }) }
    verify(exactly = 0) { sourceService.listWorkspaceSourceConnection(extra.workspaceId) }
  }

  private fun workspace(
    name: String,
    createdAt: Long,
    id: UUID = UUID.randomUUID(),
    dataplaneGroup: UUID = dataplaneGroupId,
  ): StandardWorkspace =
    StandardWorkspace()
      .withWorkspaceId(id)
      .withOrganizationId(orgId.value)
      .withName(name)
      .withCreatedAt(createdAt)
      .withDataplaneGroupId(dataplaneGroup)
      .withTombstone(false)

  private fun permission(
    userId: UUID,
    workspaceId: UUID,
  ): Permission =
    Permission()
      .withPermissionId(UUID.randomUUID())
      .withUserId(userId)
      .withWorkspaceId(workspaceId)
      .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
}
