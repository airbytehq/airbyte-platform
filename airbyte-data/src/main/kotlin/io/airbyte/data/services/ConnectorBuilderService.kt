/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.ActorDefinitionConfigInjection
import io.airbyte.config.ConnectorBuilderProject
import io.airbyte.config.ConnectorBuilderProjectVersionedManifest
import io.airbyte.config.DeclarativeManifest
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.protocol.models.v0.ConnectorSpecification
import java.io.IOException
import java.util.Optional
import java.util.UUID
import java.util.stream.Stream

/**
 * This service is responsible for managing the lifecycle of connector builder projects.
 */
interface ConnectorBuilderService {
  fun getConnectorBuilderProject(
    builderProjectId: UUID,
    fetchManifestDraft: Boolean,
  ): ConnectorBuilderProject

  fun getConnectorBuilderProjectIdForActorDefinitionId(actorDefinitionId: UUID): Optional<UUID>

  fun getVersionedConnectorBuilderProject(
    builderProjectId: UUID,
    version: Long,
  ): ConnectorBuilderProjectVersionedManifest

  fun getConnectorBuilderProjectsByWorkspace(workspaceId: UUID): Stream<ConnectorBuilderProject>

  fun deleteBuilderProject(builderProjectId: UUID): Boolean

  fun updateBuilderProjectTestingValues(
    projectId: UUID,
    testingValues: JsonNode,
  )

  fun writeBuilderProjectDraft(
    projectId: UUID,
    workspaceId: UUID,
    name: String,
    manifestDraft: JsonNode?,
    componentsFileContent: String?,
    baseActorDefinitionVersionId: UUID?,
    contributionUrl: String?,
    contributionActorDefinitionId: UUID?,
  )

  fun deleteBuilderProjectDraft(projectId: UUID)

  fun deleteManifestDraftForActorDefinition(
    actorDefinitionId: UUID,
    workspaceId: UUID,
  )

  fun updateBuilderProjectAndActorDefinition(
    projectId: UUID,
    workspaceId: UUID,
    name: String,
    manifestDraft: JsonNode?,
    componentsFileContent: String?,
    baseActorDefinitionVersionId: UUID?,
    contributionUrl: String?,
    contributionActorDefinitionId: UUID?,
    actorDefinitionId: UUID,
  )

  fun assignActorDefinitionToConnectorBuilderProject(
    builderProjectId: UUID,
    actorDefinitionId: UUID,
  )

  fun createDeclarativeManifestAsActiveVersion(
    declarativeManifest: DeclarativeManifest,
    configInjections: List<ActorDefinitionConfigInjection>,
    connectorSpecification: ConnectorSpecification,
    cdkVersion: String,
  )

  fun setDeclarativeSourceActiveVersion(
    sourceDefinitionId: UUID,
    version: Long,
    configInjections: List<ActorDefinitionConfigInjection>,
    connectorSpecification: ConnectorSpecification,
    cdkVersion: String,
  )

  fun getActorDefinitionConfigInjections(actorDefinitionId: UUID): Stream<ActorDefinitionConfigInjection>

  fun writeActorDefinitionConfigInjectionsForPath(actorDefinitionConfigInjection: List<ActorDefinitionConfigInjection>)

  fun writeActorDefinitionConfigInjectionForPath(actorDefinitionConfigInjection: ActorDefinitionConfigInjection)

  fun insertDeclarativeManifest(declarativeManifest: DeclarativeManifest)

  fun insertActiveDeclarativeManifest(declarativeManifest: DeclarativeManifest)

  fun getDeclarativeManifestsByActorDefinitionId(actorDefinitionId: UUID): Stream<DeclarativeManifest>

  fun getDeclarativeManifestByActorDefinitionIdAndVersion(
    actorDefinitionId: UUID,
    version: Long,
  ): DeclarativeManifest

  fun getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(actorDefinitionId: UUID): DeclarativeManifest
}
