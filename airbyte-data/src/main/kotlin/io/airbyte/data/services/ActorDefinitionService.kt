/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ActorType
import io.airbyte.config.ScopeType
import io.airbyte.data.services.shared.ActorWorkspaceOrganizationIds
import java.util.Optional
import java.util.UUID

/**
 * This service is responsible for managing the lifecycle of actor definitions.
 */
interface ActorDefinitionService {
  fun getActorDefinitionIdsInUse(): Set<UUID>

  fun getActorDefinitionToProtocolVersionMap(): Map<UUID, Map.Entry<ActorType, Version>>

  fun getActorDefinitionIdsToDefaultVersionsMap(): Map<UUID, ActorDefinitionVersion>

  fun updateDeclarativeActorDefinitionVersions(
    currentImageTag: String,
    targetImageTag: String,
  ): Int

  fun writeActorDefinitionWorkspaceGrant(
    actorDefinitionId: UUID,
    scopeId: UUID,
    scopeType: ScopeType,
  )

  fun actorDefinitionWorkspaceGrantExists(
    actorDefinitionId: UUID,
    scopeId: UUID,
    scopeType: ScopeType,
  ): Boolean

  fun deleteActorDefinitionWorkspaceGrant(
    actorDefinitionId: UUID,
    scopeId: UUID,
    scopeType: ScopeType,
  )

  fun writeActorDefinitionVersion(actorDefinitionVersion: ActorDefinitionVersion): ActorDefinitionVersion

  fun getActorDefinitionVersion(
    actorDefinitionId: UUID,
    dockerImageTag: String,
  ): Optional<ActorDefinitionVersion>

  fun getActorDefinitionVersion(actorDefinitionVersionId: UUID): ActorDefinitionVersion

  fun listActorDefinitionVersionsForDefinition(actorDefinitionId: UUID): List<ActorDefinitionVersion>

  fun getActorDefinitionVersions(actorDefinitionVersionIds: List<UUID?>): List<ActorDefinitionVersion>

  fun updateActorDefinitionDefaultVersionId(
    actorDefinitionId: UUID,
    versionId: UUID,
  )

  fun getDefaultVersionForActorDefinitionIdOptional(actorDefinitionId: UUID): Optional<ActorDefinitionVersion>

  fun getActorIdsForDefinition(actorDefinitionId: UUID): List<ActorWorkspaceOrganizationIds>

  fun getIdsForActors(actorIds: List<UUID>): List<ActorWorkspaceOrganizationIds>

  fun listBreakingChangesForActorDefinition(actorDefinitionId: UUID): List<ActorDefinitionBreakingChange>

  fun setActorDefinitionVersionSupportStates(
    actorDefinitionVersionIds: List<UUID>,
    supportState: ActorDefinitionVersion.SupportState,
  )

  fun listBreakingChangesForActorDefinitionVersion(actorDefinitionVersion: ActorDefinitionVersion): List<ActorDefinitionBreakingChange>

  fun listBreakingChanges(): List<ActorDefinitionBreakingChange>

  fun scopeCanUseDefinition(
    actorDefinitionId: UUID,
    scopeId: UUID,
    scopeType: String,
  ): Boolean
}
