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
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun getConnectorBuilderProject(
    builderProjectId: UUID,
    fetchManifestDraft: Boolean,
  ): ConnectorBuilderProject

  @Throws(IOException::class)
  fun getConnectorBuilderProjectIdForActorDefinitionId(actorDefinitionId: UUID): Optional<UUID>

  @Throws(ConfigNotFoundException::class, IOException::class)
  fun getVersionedConnectorBuilderProject(
    builderProjectId: UUID,
    version: Long,
  ): ConnectorBuilderProjectVersionedManifest

  @Throws(IOException::class)
  fun getConnectorBuilderProjectsByWorkspace(workspaceId: UUID): Stream<ConnectorBuilderProject>

  @Throws(IOException::class)
  fun deleteBuilderProject(builderProjectId: UUID): Boolean

  @Throws(IOException::class)
  fun updateBuilderProjectTestingValues(
    projectId: UUID,
    testingValues: JsonNode,
  )

  @Throws(IOException::class)
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

  @Throws(IOException::class)
  fun deleteBuilderProjectDraft(projectId: UUID)

  @Throws(IOException::class)
  fun deleteManifestDraftForActorDefinition(
    actorDefinitionId: UUID,
    workspaceId: UUID,
  )

  @Throws(IOException::class)
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

  @Throws(IOException::class)
  fun assignActorDefinitionToConnectorBuilderProject(
    builderProjectId: UUID,
    actorDefinitionId: UUID,
  )

  @Throws(IOException::class)
  fun createDeclarativeManifestAsActiveVersion(
    declarativeManifest: DeclarativeManifest,
    configInjections: List<ActorDefinitionConfigInjection>,
    connectorSpecification: ConnectorSpecification,
    cdkVersion: String,
  )

  @Throws(IOException::class)
  fun setDeclarativeSourceActiveVersion(
    sourceDefinitionId: UUID,
    version: Long,
    configInjections: List<ActorDefinitionConfigInjection>,
    connectorSpecification: ConnectorSpecification,
    cdkVersion: String,
  )

  @Throws(IOException::class)
  fun getActorDefinitionConfigInjections(actorDefinitionId: UUID): Stream<ActorDefinitionConfigInjection>

  @Throws(IOException::class)
  fun writeActorDefinitionConfigInjectionsForPath(actorDefinitionConfigInjection: List<ActorDefinitionConfigInjection>)

  @Throws(IOException::class)
  fun writeActorDefinitionConfigInjectionForPath(actorDefinitionConfigInjection: ActorDefinitionConfigInjection)

  @Throws(IOException::class)
  fun insertDeclarativeManifest(declarativeManifest: DeclarativeManifest)

  @Throws(IOException::class)
  fun insertActiveDeclarativeManifest(declarativeManifest: DeclarativeManifest)

  @Throws(IOException::class)
  fun getDeclarativeManifestsByActorDefinitionId(actorDefinitionId: UUID): Stream<DeclarativeManifest>

  @Throws(IOException::class, ConfigNotFoundException::class)
  fun getDeclarativeManifestByActorDefinitionIdAndVersion(
    actorDefinitionId: UUID,
    version: Long,
  ): DeclarativeManifest

  @Throws(IOException::class, ConfigNotFoundException::class)
  fun getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(actorDefinitionId: UUID): DeclarativeManifest
}
