/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.commons.constants.AirbyteCatalogConstants
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectorBuilderService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import jakarta.inject.Singleton
import java.util.UUID

data class PublicDefinitionMetadata(
  val custom: Boolean,
  val declarative: Boolean,
)

@Singleton
class PublicDefinitionAuthorizationService(
  private val sources: SourceService,
  private val destinations: DestinationService,
  private val projects: ConnectorBuilderService,
  private val actors: ActorDefinitionService,
) {
  fun definition(
    id: UUID,
    source: Boolean,
  ): PublicDefinitionMetadata =
    resource {
      if (source) {
        val definition = sources.getStandardSourceDefinition(id)
        if (definition.sourceDefinitionId != id || definition.tombstone == true) throw AuthorizationResourceNotFoundException()
        val version = actors.getDefaultVersionForActorDefinitionIdOptional(id).orElseThrow { AuthorizationResourceNotFoundException() }
        if (version.actorDefinitionId != id) throw AuthorizationResourceNotFoundException()
        PublicDefinitionMetadata(
          definition.custom == true,
          version.dockerRepository == AirbyteCatalogConstants.AIRBYTE_SOURCE_DECLARATIVE_MANIFEST_IMAGE,
        )
      } else {
        val definition = destinations.getStandardDestinationDefinition(id)
        if (definition.destinationDefinitionId != id || definition.tombstone == true) throw AuthorizationResourceNotFoundException()
        PublicDefinitionMetadata(definition.custom == true, false)
      }
    }

  fun builderWorkspace(definition: UUID): UUID =
    resource {
      val projectId = projects.getConnectorBuilderProjectIdForActorDefinitionId(definition).orElseThrow { AuthorizationResourceNotFoundException() }
      val project = projects.getConnectorBuilderProjectMetadata(projectId)
      if (project.id != projectId ||
        project.definition.orElseThrow { AuthorizationResourceNotFoundException() } != definition
      ) {
        throw AuthorizationResourceNotFoundException()
      }
      project.workspace.orElseThrow { AuthorizationResourceNotFoundException() }
    }

  private fun <T> resource(lookup: () -> T): T =
    try {
      lookup()
    } catch (_: ConfigNotFoundException) {
      throw AuthorizationResourceNotFoundException()
    }
}
