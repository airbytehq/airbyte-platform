/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.fusion

import io.airbyte.config.StandardSync
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.shared.StandardSyncQuery
import io.airbyte.domain.models.fusion.SkillDocsActor
import io.airbyte.domain.models.fusion.SkillDocsSource
import jakarta.inject.Singleton
import java.util.UUID

/**
 * Workspace-scoped actor and connection reads for Sonar skill docs. Missing, tombstoned and other-workspace actors are
 * indistinguishable to callers.
 */
@Singleton
class SkillDocsActorService(
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
  private val connectionService: ConnectionService,
) {
  fun getActor(
    workspaceId: UUID,
    actorId: UUID,
    isDestination: Boolean,
  ): SkillDocsActor? =
    try {
      if (isDestination) {
        destinationService
          .getDestinationConnection(actorId)
          .takeIf { it.tombstone == false && it.workspaceId == workspaceId }
          ?.let { SkillDocsActor(it.destinationDefinitionId, it.name) }
      } else {
        sourceService
          .getSourceConnection(actorId)
          .takeIf { it.tombstone == false && it.workspaceId == workspaceId }
          ?.let { SkillDocsActor(it.sourceDefinitionId, it.name) }
      }
    } catch (_: ConfigNotFoundException) {
      null
    } catch (_: io.airbyte.config.persistence.ConfigNotFoundException) {
      null
    }

  fun listDestinationSyncs(
    workspaceId: UUID,
    destinationId: UUID,
    sourceId: UUID?,
  ): List<StandardSync> =
    connectionService.listWorkspaceStandardSyncs(StandardSyncQuery(workspaceId, sourceId?.let { listOf(it) }, listOf(destinationId), false))

  fun listWorkspaceSources(
    workspaceId: UUID,
    sourceIds: List<UUID>,
  ): List<SkillDocsSource> {
    if (sourceIds.isEmpty()) return emptyList()
    return sourceService
      .getSourceAndDefinitionsFromSourceIds(sourceIds)
      .filter { it.source.workspaceId == workspaceId && it.source.tombstone == false }
      .map { SkillDocsSource(it.source.sourceId, it.source.name, it.definition.name) }
  }
}
