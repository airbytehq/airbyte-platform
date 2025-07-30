/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.DestinationDefinitionRead
import io.airbyte.api.model.generated.SourceDefinitionRead
import io.airbyte.api.model.generated.WebBackendCheckUpdatesRead
import io.airbyte.commons.lang.Exceptions
import io.airbyte.config.ConnectorRegistryDestinationDefinition
import io.airbyte.config.ConnectorRegistrySourceDefinition
import io.airbyte.config.specs.RemoteDefinitionsProvider
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.io.IOException
import java.util.UUID
import java.util.function.Function
import java.util.stream.Collectors

/**
 * The web backend is an abstraction that allows the frontend to structure data in such a way that
 * it is easier for a react frontend to consume. It should NOT have direct access to the database.
 * It should operate exclusively by calling other endpoints that are exposed in the API.
 *
 * Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
open class WebBackendCheckUpdatesHandler(
  val sourceDefinitionsHandler: SourceDefinitionsHandler,
  val destinationDefinitionsHandler: DestinationDefinitionsHandler,
  val remoteDefinitionsProvider: RemoteDefinitionsProvider,
) {
  fun checkUpdates(): WebBackendCheckUpdatesRead {
    val destinationDiffCount = destinationDiffCount
    val sourceDiffCount = sourceDiffCount

    return WebBackendCheckUpdatesRead()
      .destinationDefinitions(destinationDiffCount)
      .sourceDefinitions(sourceDiffCount)
  }

  private val destinationDiffCount: Int
    get() {
      val currentActorDefToDockerImageTag: List<Map.Entry<UUID?, String>>
      val newActorDefToDockerImageTag: Map<UUID?, String>

      try {
        currentActorDefToDockerImageTag =
          destinationDefinitionsHandler
            .listDestinationDefinitions()
            .destinationDefinitions
            .stream()
            .map { def: DestinationDefinitionRead -> java.util.Map.entry(def.destinationDefinitionId, def.dockerImageTag) }
            .toList()
      } catch (e: IOException) {
        log.error("Failed to get current list of standard destination definitions", e)
        return NO_CHANGES_FOUND
      }

      val latestDestinationDefinitions =
        Exceptions.swallowWithDefault({ remoteDefinitionsProvider.getDestinationDefinitions() }, emptyList())
      newActorDefToDockerImageTag =
        latestDestinationDefinitions
          .stream()
          .collect(
            Collectors.toMap(
              Function { obj: ConnectorRegistryDestinationDefinition -> obj.destinationDefinitionId },
              Function { obj: ConnectorRegistryDestinationDefinition -> obj.dockerImageTag },
            ),
          )

      return getDiffCount(currentActorDefToDockerImageTag, newActorDefToDockerImageTag)
    }

  private val sourceDiffCount: Int
    get() {
      val currentActorDefToDockerImageTag: List<Map.Entry<UUID?, String>>
      val newActorDefToDockerImageTag: Map<UUID?, String>

      try {
        currentActorDefToDockerImageTag =
          sourceDefinitionsHandler
            .listSourceDefinitions()
            .sourceDefinitions
            .stream()
            .map { def: SourceDefinitionRead -> java.util.Map.entry(def.sourceDefinitionId, def.dockerImageTag) }
            .toList()
      } catch (e: IOException) {
        log.error("Failed to get current list of standard source definitions", e)
        return NO_CHANGES_FOUND
      }

      val latestSourceDefinitions =
        Exceptions.swallowWithDefault({ remoteDefinitionsProvider.getSourceDefinitions() }, emptyList())
      newActorDefToDockerImageTag =
        latestSourceDefinitions
          .stream()
          .collect(
            Collectors.toMap(
              Function { obj: ConnectorRegistrySourceDefinition -> obj.sourceDefinitionId },
              Function { obj: ConnectorRegistrySourceDefinition -> obj.dockerImageTag },
            ),
          )

      return getDiffCount(currentActorDefToDockerImageTag, newActorDefToDockerImageTag)
    }

  private fun getDiffCount(
    initialSet: List<Map.Entry<UUID?, String>>,
    newSet: Map<UUID?, String>,
  ): Int {
    var diffCount = 0
    for ((key, value) in initialSet) {
      val newDockerImageTag = newSet[key]
      if (newDockerImageTag != null && value != newDockerImageTag) {
        ++diffCount
      }
    }
    return diffCount
  }

  companion object {
    private val log = KotlinLogging.logger {}

    private const val NO_CHANGES_FOUND = 0
  }
}
