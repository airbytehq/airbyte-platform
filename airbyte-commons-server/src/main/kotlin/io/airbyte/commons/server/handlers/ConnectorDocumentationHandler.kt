/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.ActorType
import io.airbyte.api.model.generated.ConnectorDocumentationRead
import io.airbyte.api.model.generated.ConnectorDocumentationRequestBody
import io.airbyte.commons.server.errors.NotFoundException
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.specs.RemoteDefinitionsProvider
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.validation.json.JsonValidationException
import jakarta.annotation.Nullable
import jakarta.inject.Singleton
import java.io.IOException
import java.util.UUID

/**
 * ConnectorDocumentationHandler. Javadocs suppressed because api docs should be used as source of
 * truth.
 */
@Singleton
open class ConnectorDocumentationHandler(
  private val actorDefinitionVersionHelper: ActorDefinitionVersionHelper,
  private val remoteDefinitionsProvider: RemoteDefinitionsProvider,
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
) {
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, ConfigNotFoundException::class)
  fun getConnectorDocumentation(request: ConnectorDocumentationRequestBody): ConnectorDocumentationRead {
    val actorDefinitionVersion =
      if (request.actorType == ActorType.SOURCE) {
        getSourceActorDefinitionVersion(request.actorDefinitionId, request.workspaceId, request.actorId)
      } else {
        getDestinationActorDefinitionVersion(request.actorDefinitionId, request.workspaceId, request.actorId)
      }
    val dockerRepo = actorDefinitionVersion.dockerRepository
    val version = actorDefinitionVersion.dockerImageTag

    // prioritize versioned over latest
    val versionedDocString = remoteDefinitionsProvider.getConnectorDocumentation(dockerRepo, version)
    if (versionedDocString.isPresent) {
      return ConnectorDocumentationRead().doc(versionedDocString.get())
    }

    val latestDocString = remoteDefinitionsProvider.getConnectorDocumentation(dockerRepo, LATEST)
    if (latestDocString.isPresent) {
      return ConnectorDocumentationRead().doc(latestDocString.get())
    }

    throw NotFoundException(String.format("Could not find any documentation for connector %s", dockerRepo))
  }

  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  private fun getSourceActorDefinitionVersion(
    sourceDefinitionId: UUID,
    workspaceId: UUID,
    @Nullable sourceId: UUID?,
  ): ActorDefinitionVersion {
    val sourceDefinition = sourceService.getStandardSourceDefinition(sourceDefinitionId)
    return actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, workspaceId, sourceId)
  }

  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, ConfigNotFoundException::class)
  private fun getDestinationActorDefinitionVersion(
    destDefinitionId: UUID,
    workspaceId: UUID,
    @Nullable destId: UUID?,
  ): ActorDefinitionVersion {
    val destDefinition = destinationService.getStandardDestinationDefinition(destDefinitionId)
    return actorDefinitionVersionHelper.getDestinationVersion(destDefinition, workspaceId, destId)
  }

  companion object {
    const val LATEST: String = "latest"
  }
}
