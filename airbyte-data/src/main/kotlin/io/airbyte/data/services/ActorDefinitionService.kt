/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ActorType
import io.airbyte.config.ScopeType
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.shared.ActorWorkspaceOrganizationIds
import java.io.IOException
import java.util.Optional
import java.util.UUID

/**
 * This service is responsible for managing the lifecycle of actor definitions.
 */
interface ActorDefinitionService {
  @Throws(IOException::class)
  fun getActorDefinitionIdsInUse(): Set<UUID>

  @Throws(IOException::class)
  fun getActorDefinitionToProtocolVersionMap(): Map<UUID, Map.Entry<ActorType, Version>>

  @Throws(IOException::class)
  fun getActorDefinitionIdsToDefaultVersionsMap(): Map<UUID, ActorDefinitionVersion>

  @Throws(IOException::class)
  fun updateDeclarativeActorDefinitionVersions(
    currentImageTag: String,
    targetImageTag: String,
  ): Int

  @Throws(IOException::class)
  fun writeActorDefinitionWorkspaceGrant(
    actorDefinitionId: UUID,
    scopeId: UUID,
    scopeType: ScopeType,
  )

  @Throws(IOException::class)
  fun actorDefinitionWorkspaceGrantExists(
    actorDefinitionId: UUID,
    scopeId: UUID,
    scopeType: ScopeType,
  ): Boolean

  @Throws(IOException::class)
  fun deleteActorDefinitionWorkspaceGrant(
    actorDefinitionId: UUID,
    scopeId: UUID,
    scopeType: ScopeType,
  )

  @Throws(IOException::class)
  fun writeActorDefinitionVersion(actorDefinitionVersion: ActorDefinitionVersion): ActorDefinitionVersion

  @Throws(IOException::class)
  fun getActorDefinitionVersion(
    actorDefinitionId: UUID,
    dockerImageTag: String,
  ): Optional<ActorDefinitionVersion>

  @Throws(IOException::class, ConfigNotFoundException::class)
  fun getActorDefinitionVersion(actorDefinitionVersionId: UUID): ActorDefinitionVersion

  @Throws(IOException::class)
  fun listActorDefinitionVersionsForDefinition(actorDefinitionId: UUID): List<ActorDefinitionVersion>

  @Throws(IOException::class)
  fun getActorDefinitionVersions(actorDefinitionVersionIds: List<UUID?>): List<ActorDefinitionVersion>

  @Throws(IOException::class)
  fun updateActorDefinitionDefaultVersionId(
    actorDefinitionId: UUID,
    versionId: UUID,
  )

  @Throws(IOException::class)
  fun getDefaultVersionForActorDefinitionIdOptional(actorDefinitionId: UUID): Optional<ActorDefinitionVersion>

  @Throws(IOException::class)
  fun getActorIdsForDefinition(actorDefinitionId: UUID): List<ActorWorkspaceOrganizationIds>

  @Throws(IOException::class)
  fun getIdsForActors(actorIds: List<UUID>): List<ActorWorkspaceOrganizationIds>

  @Throws(IOException::class)
  fun listBreakingChangesForActorDefinition(actorDefinitionId: UUID): List<ActorDefinitionBreakingChange>

  @Throws(IOException::class)
  fun setActorDefinitionVersionSupportStates(
    actorDefinitionVersionIds: List<UUID>,
    supportState: ActorDefinitionVersion.SupportState,
  )

  @Throws(IOException::class)
  fun listBreakingChangesForActorDefinitionVersion(actorDefinitionVersion: ActorDefinitionVersion): List<ActorDefinitionBreakingChange>

  @Throws(IOException::class)
  fun listBreakingChanges(): List<ActorDefinitionBreakingChange>

  @Throws(IOException::class)
  fun scopeCanUseDefinition(
    actorDefinitionId: UUID,
    scopeId: UUID,
    scopeType: String,
  ): Boolean
}
