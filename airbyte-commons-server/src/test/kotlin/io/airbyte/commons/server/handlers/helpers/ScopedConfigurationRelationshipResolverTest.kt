/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.DestinationConnection
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.persistence.WorkspacePersistence
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.shared.ConnectorVersionKey
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.Optional
import java.util.UUID

internal class ScopedConfigurationRelationshipResolverTest {
  private val workspaceService = mockk<WorkspaceService>()
  private val workspacePersistence = mockk<WorkspacePersistence>()
  private val sourceService = mockk<SourceService>()
  private val destinationService = mockk<DestinationService>()
  private val scopedConfigurationRelationshipResolver =
    ScopedConfigurationRelationshipResolver(
      workspaceService,
      workspacePersistence,
      sourceService,
      destinationService,
    )

  @BeforeEach
  fun reset() {
    clearAllMocks()
  }

  @Test
  fun `test get workspace parent organization`() {
    val workspaceId = UUID.randomUUID()
    val orgId = UUID.randomUUID()

    every { workspaceService.getOrganizationIdFromWorkspaceId(workspaceId) } returns Optional.of(orgId)

    val parentScopeId =
      scopedConfigurationRelationshipResolver.getParentScopeId(
        ConfigScopeType.WORKSPACE,
        ConfigScopeType.ORGANIZATION,
        workspaceId,
      )
    assertEquals(orgId, parentScopeId)

    verify {
      workspaceService.getOrganizationIdFromWorkspaceId(workspaceId)
    }
  }

  @Test
  fun `test get source parent workspace`() {
    val sourceId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()

    every { sourceService.getSourceConnection(sourceId) } returns SourceConnection().withWorkspaceId(workspaceId)

    val parentScopeId = scopedConfigurationRelationshipResolver.getParentScopeId(ConfigScopeType.ACTOR, ConfigScopeType.WORKSPACE, sourceId)
    assertEquals(workspaceId, parentScopeId)

    verify {
      sourceService.getSourceConnection(sourceId)
    }
  }

  @Test
  fun `test get destination parent workspace`() {
    val destinationId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()

    every { sourceService.getSourceConnection(destinationId) } throws ConfigNotFoundException(ConfigNotFoundType.SOURCE_CONNECTION, destinationId)
    every { destinationService.getDestinationConnection(destinationId) } returns DestinationConnection().withWorkspaceId(workspaceId)

    val parentScopeId = scopedConfigurationRelationshipResolver.getParentScopeId(ConfigScopeType.ACTOR, ConfigScopeType.WORKSPACE, destinationId)
    assertEquals(workspaceId, parentScopeId)

    verifyAll {
      sourceService.getSourceConnection(destinationId)
      destinationService.getDestinationConnection(destinationId)
    }
  }

  @Test
  fun `test get organization child workspaces`() {
    val orgId = UUID.randomUUID()
    val workspaceId1 = UUID.randomUUID()
    val workspaceId2 = UUID.randomUUID()

    every {
      workspacePersistence.listWorkspacesByOrganizationId(orgId, true, Optional.empty())
    } returns listOf(StandardWorkspace().withWorkspaceId(workspaceId1), StandardWorkspace().withWorkspaceId(workspaceId2))

    val childScopeIds = scopedConfigurationRelationshipResolver.getChildScopeIds(ConfigScopeType.ORGANIZATION, ConfigScopeType.WORKSPACE, orgId)
    assertEquals(listOf(workspaceId1, workspaceId2), childScopeIds)

    verifyAll {
      workspacePersistence.listWorkspacesByOrganizationId(orgId, true, Optional.empty())
    }
  }

  @Test
  fun `test get workspace child actors`() {
    val workspaceId = UUID.randomUUID()
    val sourceId1 = UUID.randomUUID()
    val sourceId2 = UUID.randomUUID()
    val destinationId = UUID.randomUUID()

    every {
      sourceService.listWorkspaceSourceConnection(workspaceId)
    } returns listOf(SourceConnection().withSourceId(sourceId1), SourceConnection().withSourceId(sourceId2))
    every {
      destinationService.listWorkspaceDestinationConnection(workspaceId)
    } returns listOf(DestinationConnection().withDestinationId(destinationId))

    val childScopeIds = scopedConfigurationRelationshipResolver.getChildScopeIds(ConfigScopeType.WORKSPACE, ConfigScopeType.ACTOR, workspaceId)
    assertEquals(listOf(sourceId1, sourceId2, destinationId), childScopeIds)

    verifyAll {
      sourceService.listWorkspaceSourceConnection(workspaceId)
      destinationService.listWorkspaceDestinationConnection(workspaceId)
    }
  }

  @Test
  fun `test get parent for unsupported parent scope type throws`() {
    assertThrows<IllegalArgumentException> {
      scopedConfigurationRelationshipResolver.getParentScopeId(ConfigScopeType.WORKSPACE, ConfigScopeType.ACTOR, UUID.randomUUID())
    }
  }

  @Test
  fun `test get child for unsupported child scope type throws`() {
    assertThrows<IllegalArgumentException> {
      scopedConfigurationRelationshipResolver.getChildScopeIds(ConfigScopeType.ACTOR, ConfigScopeType.ORGANIZATION, UUID.randomUUID())
    }
  }

  @Test
  fun `test get all ancestor scopes`() {
    val workspaceId = UUID.randomUUID()
    val orgId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()

    every { sourceService.getSourceConnection(sourceId) } returns SourceConnection().withWorkspaceId(workspaceId)
    every { workspaceService.getOrganizationIdFromWorkspaceId(workspaceId) } returns Optional.of(orgId)

    val ancestorScopes =
      scopedConfigurationRelationshipResolver.getAllAncestorScopes(
        ConnectorVersionKey.supportedScopes,
        ConfigScopeType.ACTOR,
        sourceId,
      )
    assertEquals(mapOf(ConfigScopeType.ORGANIZATION to orgId, ConfigScopeType.WORKSPACE to workspaceId), ancestorScopes)

    verifyAll {
      sourceService.getSourceConnection(sourceId)
      workspaceService.getOrganizationIdFromWorkspaceId(workspaceId)
    }
  }

  @Test
  fun `test get all descendant scopes`() {
    val orgId = UUID.randomUUID()

    val workspaceId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()

    val workspaceId2 = UUID.randomUUID()
    val sourceId2 = UUID.randomUUID()

    every { workspacePersistence.listWorkspacesByOrganizationId(orgId, true, Optional.empty()) } returns
      listOf(
        StandardWorkspace().withWorkspaceId(workspaceId),
        StandardWorkspace().withWorkspaceId(workspaceId2),
      )
    every { sourceService.listWorkspaceSourceConnection(workspaceId) } returns listOf(SourceConnection().withSourceId(sourceId))
    every { destinationService.listWorkspaceDestinationConnection(workspaceId) } returns
      listOf(DestinationConnection().withDestinationId(destinationId))
    every { sourceService.listWorkspaceSourceConnection(workspaceId2) } returns listOf(SourceConnection().withSourceId(sourceId2))
    every { destinationService.listWorkspaceDestinationConnection(workspaceId2) } returns emptyList()

    val descendantScopes =
      scopedConfigurationRelationshipResolver.getAllDescendantScopes(
        ConnectorVersionKey.supportedScopes,
        ConfigScopeType.ORGANIZATION,
        orgId,
      )
    assertEquals(
      mapOf(
        ConfigScopeType.WORKSPACE to listOf(workspaceId, workspaceId2),
        ConfigScopeType.ACTOR to listOf(sourceId, destinationId, sourceId2),
      ),
      descendantScopes,
    )

    verifyAll {
      workspacePersistence.listWorkspacesByOrganizationId(orgId, true, Optional.empty())
      sourceService.listWorkspaceSourceConnection(workspaceId)
      destinationService.listWorkspaceDestinationConnection(workspaceId)
      sourceService.listWorkspaceSourceConnection(workspaceId2)
      destinationService.listWorkspaceDestinationConnection(workspaceId2)
    }
  }
}
