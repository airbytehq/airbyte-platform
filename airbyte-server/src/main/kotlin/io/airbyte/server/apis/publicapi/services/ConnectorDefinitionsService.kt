package io.airbyte.server.apis.publicapi.services

import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.commons.server.handlers.DestinationDefinitionsHandler
import io.airbyte.commons.server.handlers.SourceDefinitionsHandler
import io.airbyte.publicApi.server.generated.models.ConnectorDefinitionsResponse
import io.airbyte.publicApi.server.generated.models.ConnectorType
import io.airbyte.server.apis.publicapi.mappers.toPublicApiModel
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
          return sourceDefinitionsHandler.listSourceDefinitionsForWorkspace(WorkspaceIdRequestBody().workspaceId(workspaceId)).toPublicApiModel()
        }
        return sourceDefinitionsHandler.listPublicSourceDefinitions().toPublicApiModel()
      }
      ConnectorType.DESTINATION -> {
        if (workspaceId != null) {
          return destinationDefinitionsHandler.listDestinationDefinitionsForWorkspace(
            WorkspaceIdRequestBody().workspaceId(workspaceId),
          ).toPublicApiModel()
        }
        return destinationDefinitionsHandler.listPublicDestinationDefinitions().toPublicApiModel()
      }
    }
  }
}
