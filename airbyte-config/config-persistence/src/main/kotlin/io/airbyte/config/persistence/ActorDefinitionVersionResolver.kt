/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectorRegistryDestinationDefinition
import io.airbyte.config.ConnectorRegistrySourceDefinition
import io.airbyte.config.helpers.ConnectorRegistryConverters.toActorDefinitionVersion
import io.airbyte.config.specs.RemoteDefinitionsProvider
import io.airbyte.data.services.ActorDefinitionService
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.io.IOException
import java.util.Optional
import java.util.UUID

/**
 * Helper class for resolving ActorDefinitionVersions based on their existence locally in the
 * database or remotely in the DefinitionsProvider.
 */
@Singleton
class ActorDefinitionVersionResolver(
  private val remoteDefinitionsProvider: RemoteDefinitionsProvider,
  private val actorDefinitionService: ActorDefinitionService,
) {
  /**
   * Resolves an ActorDefinitionVersion for a given actor definition id and docker image tag. If the
   * ADV does not already exist in the database, it will attempt to fetch the associated registry
   * definition from the DefinitionsProvider and use it to write a new ADV to the database.
   *
   * @return ActorDefinitionVersion if the version was resolved, otherwise empty optional
   */
  @Throws(IOException::class)
  fun resolveVersionForTag(
    actorDefinitionId: UUID,
    actorType: ActorType,
    dockerRepository: String?,
    dockerImageTag: String,
  ): Optional<ActorDefinitionVersion> {
    val existingVersion =
      actorDefinitionService.getActorDefinitionVersion(actorDefinitionId, dockerImageTag)
    if (existingVersion.isPresent) {
      return existingVersion
    }

    val registryDefinitionVersion =
      fetchRemoteActorDefinitionVersion(actorType, dockerRepository, dockerImageTag)
    if (registryDefinitionVersion.isEmpty) {
      return Optional.empty()
    }

    val newVersion = registryDefinitionVersion.get().withActorDefinitionId(actorDefinitionId)
    val persistedADV = actorDefinitionService.writeActorDefinitionVersion(newVersion)
    log.info { "Persisted new version {} for definition {} with tag $persistedADV.versionId, actorDefinitionId, dockerImageTag" }

    return Optional.of(persistedADV)
  }

  fun fetchRemoteActorDefinitionVersion(
    actorType: ActorType,
    connectorRepository: String?,
    dockerImageTag: String?,
  ): Optional<ActorDefinitionVersion> {
    val actorDefinitionVersion: Optional<ActorDefinitionVersion>
    when (actorType) {
      ActorType.SOURCE -> {
        val registryDef =
          remoteDefinitionsProvider.getSourceDefinitionByVersion(connectorRepository, dockerImageTag)
        actorDefinitionVersion =
          registryDef.map { obj: ConnectorRegistrySourceDefinition -> toActorDefinitionVersion(obj) }
      }

      ActorType.DESTINATION -> {
        val registryDef =
          remoteDefinitionsProvider.getDestinationDefinitionByVersion(connectorRepository, dockerImageTag)
        actorDefinitionVersion =
          registryDef.map { obj: ConnectorRegistryDestinationDefinition -> toActorDefinitionVersion(obj) }
      }

      else -> throw IllegalArgumentException("Actor type not supported: $actorType")
    }

    if (actorDefinitionVersion.isEmpty) {
      log.error(dockerImageTag) { "Failed to fetch registry entry for {}:$connectorRepository" }
      return Optional.empty()
    }

    log.info { "Fetched registry entry for {}:$connectorRepository, dockerImageTag." }
    return actorDefinitionVersion
  }

  companion object {
    private val log = KotlinLogging.logger {}
  }
}
