/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.services

import io.airbyte.api.model.generated.DestinationDefinitionRead
import io.airbyte.api.model.generated.DestinationDefinitionReadList
import io.airbyte.api.model.generated.SourceDefinitionRead
import io.airbyte.api.model.generated.SourceDefinitionReadList
import io.airbyte.api.model.generated.WorkspaceIdActorDefinitionRequestBody
import io.airbyte.commons.server.handlers.DestinationDefinitionsHandler
import io.airbyte.commons.server.handlers.SourceDefinitionsHandler
import io.airbyte.publicApi.server.generated.models.ConnectorDefinitionResponse
import io.airbyte.publicApi.server.generated.models.ConnectorDefinitionsResponse
import io.airbyte.publicApi.server.generated.models.ConnectorType
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Singleton
import java.util.UUID

interface ConnectorDefinitionsService {
  fun listConnectorDefinitions(
    type: ConnectorType,
    workspaceId: UUID?,
  ): ConnectorDefinitionsResponse
}

@Secondary
@Singleton
class ConnectorDefinitionsServiceImpl(
  val sourceDefinitionsHandler: SourceDefinitionsHandler,
  val destinationDefinitionsHandler: DestinationDefinitionsHandler,
) : ConnectorDefinitionsService {
  override fun listConnectorDefinitions(
    type: ConnectorType,
    workspaceId: UUID?,
  ): ConnectorDefinitionsResponse {
    when (type) {
      ConnectorType.SOURCE -> {
        if (workspaceId != null) {
          return sourceDefinitionsHandler
            .listSourceDefinitionsForWorkspace(
              WorkspaceIdActorDefinitionRequestBody().workspaceId(workspaceId),
            ).toPublicApiModel()
        }
        return sourceDefinitionsHandler.listPublicSourceDefinitions().toPublicApiModel()
      }
      ConnectorType.DESTINATION -> {
        if (workspaceId != null) {
          return destinationDefinitionsHandler
            .listDestinationDefinitionsForWorkspace(
              WorkspaceIdActorDefinitionRequestBody().workspaceId(workspaceId),
            ).toPublicApiModel()
        }
        return destinationDefinitionsHandler.listPublicDestinationDefinitions().toPublicApiModel()
      }
    }
  }
}

private fun SourceDefinitionRead.toPublicApiModel(): ConnectorDefinitionResponse =
  ConnectorDefinitionResponse(
    id = this.sourceDefinitionId.toString(),
    connectorDefinitionType = ConnectorType.SOURCE,
    name = this.name,
    version = this.dockerImageTag,
  )

private fun SourceDefinitionReadList.toPublicApiModel(): ConnectorDefinitionsResponse =
  ConnectorDefinitionsResponse(
    data = this.sourceDefinitions.map { it.toPublicApiModel() },
  )

private fun DestinationDefinitionRead.toPublicApiModel(): ConnectorDefinitionResponse =
  ConnectorDefinitionResponse(
    id = this.destinationDefinitionId.toString(),
    name = this.name,
    connectorDefinitionType = ConnectorType.DESTINATION,
    version = this.dockerImageTag,
  )

private fun DestinationDefinitionReadList.toPublicApiModel(): ConnectorDefinitionsResponse =
  ConnectorDefinitionsResponse(
    data = this.destinationDefinitions.map { it.toPublicApiModel() },
  )
