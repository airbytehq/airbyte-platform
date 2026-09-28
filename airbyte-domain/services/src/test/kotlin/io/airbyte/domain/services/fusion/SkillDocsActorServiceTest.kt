/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.fusion

import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.DestinationConnection
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.shared.SourceAndDefinition
import io.airbyte.data.services.shared.StandardSyncQuery
import io.airbyte.domain.models.fusion.SkillDocsActor
import io.airbyte.domain.models.fusion.SkillDocsSource
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException
import java.util.UUID

class SkillDocsActorServiceTest {
  private val sourceService = mockk<SourceService>()
  private val destinationService = mockk<DestinationService>()
  private val connectionService = mockk<ConnectionService>()
  private val service = SkillDocsActorService(sourceService, destinationService, connectionService)
  private val workspaceId = UUID.randomUUID()
  private val actorId = UUID.randomUUID()
  private val definitionId = UUID.randomUUID()

  private fun source(
    workspace: UUID = workspaceId,
    tombstone: Boolean? = false,
  ) = SourceConnection()
    .withSourceId(actorId)
    .withWorkspaceId(workspace)
    .withSourceDefinitionId(definitionId)
    .withName("My GitHub")
    .withTombstone(tombstone)

  private fun destination(
    workspace: UUID = workspaceId,
    tombstone: Boolean? = false,
  ) = DestinationConnection()
    .withDestinationId(actorId)
    .withWorkspaceId(workspace)
    .withDestinationDefinitionId(definitionId)
    .withName("My warehouse")
    .withTombstone(tombstone)

  @Test
  fun `actors in the workspace return their definition and name`() {
    every { sourceService.getSourceConnection(actorId) } returns source()
    every { destinationService.getDestinationConnection(actorId) } returns destination()
    assertEquals(SkillDocsActor(definitionId, "My GitHub"), service.getActor(workspaceId, actorId, false))
    assertEquals(SkillDocsActor(definitionId, "My warehouse"), service.getActor(workspaceId, actorId, true))
  }

  @Test
  fun `foreign tombstoned and missing actors are null`() {
    for (other in listOf(destination(workspace = UUID.randomUUID()), destination(tombstone = true), destination(tombstone = null))) {
      every { destinationService.getDestinationConnection(actorId) } returns other
      assertNull(service.getActor(workspaceId, actorId, true))
    }
    for (other in listOf(source(workspace = UUID.randomUUID()), source(tombstone = true), source(tombstone = null))) {
      every { sourceService.getSourceConnection(actorId) } returns other
      assertNull(service.getActor(workspaceId, actorId, false))
    }
    every { sourceService.getSourceConnection(actorId) } throws ConfigNotFoundException(ConfigNotFoundType.SOURCE_CONNECTION, actorId)
    assertNull(service.getActor(workspaceId, actorId, false))
    every { destinationService.getDestinationConnection(actorId) } throws
      io.airbyte.config.persistence
        .ConfigNotFoundException(ConfigNotFoundType.DESTINATION_CONNECTION, actorId)
    assertNull(service.getActor(workspaceId, actorId, true))
  }

  @Test
  fun `actor read failures propagate`() {
    every { sourceService.getSourceConnection(actorId) } throws IOException("database unavailable")
    assertThrows<IOException> { service.getActor(workspaceId, actorId, false) }
    every { destinationService.getDestinationConnection(actorId) } throws IOException("database unavailable")
    assertThrows<IOException> { service.getActor(workspaceId, actorId, true) }
  }

  @Test
  fun `destination syncs are queried by workspace destination and optional source without deleted`() {
    val sourceId = UUID.randomUUID()
    val syncs = listOf(StandardSync().withConnectionId(UUID.randomUUID()))
    every { connectionService.listWorkspaceStandardSyncs(any<StandardSyncQuery>()) } returns syncs
    assertEquals(syncs, service.listDestinationSyncs(workspaceId, actorId, null))
    assertEquals(syncs, service.listDestinationSyncs(workspaceId, actorId, sourceId))
    verify(exactly = 1) { connectionService.listWorkspaceStandardSyncs(StandardSyncQuery(workspaceId, null, listOf(actorId), false)) }
    verify(exactly = 1) {
      connectionService.listWorkspaceStandardSyncs(StandardSyncQuery(workspaceId, listOf(sourceId), listOf(actorId), false))
    }
  }

  @Test
  fun `workspace sources exclude foreign and tombstoned sources`() {
    val sourceId = UUID.randomUUID()

    fun entry(
      name: String,
      workspace: UUID,
      tombstone: Boolean?,
    ) = SourceAndDefinition(
      SourceConnection()
        .withSourceId(sourceId)
        .withWorkspaceId(workspace)
        .withName(name)
        .withTombstone(tombstone),
      StandardSourceDefinition().withName("$name definition"),
    )
    every { sourceService.getSourceAndDefinitionsFromSourceIds(listOf(sourceId)) } returns
      listOf(
        entry("Orders", workspaceId, false),
        entry("Foreign", UUID.randomUUID(), false),
        entry("Deleted", workspaceId, true),
        entry("Unknown", workspaceId, null),
      )
    assertEquals(listOf(SkillDocsSource(sourceId, "Orders", "Orders definition")), service.listWorkspaceSources(workspaceId, listOf(sourceId)))
  }

  @Test
  fun `empty source ids make no source read`() {
    assertEquals(emptyList<SkillDocsSource>(), service.listWorkspaceSources(workspaceId, emptyList()))
    verify(exactly = 0) { sourceService.getSourceAndDefinitionsFromSourceIds(any()) }
  }
}
