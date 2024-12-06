package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.DestinationDefinitionRead
import io.airbyte.api.model.generated.DestinationDefinitionReadList
import io.airbyte.publicApi.server.generated.models.ConnectorDefinitionResponse
import io.airbyte.publicApi.server.generated.models.ConnectorDefinitionsResponse
import io.airbyte.publicApi.server.generated.models.ConnectorType

fun DestinationDefinitionRead.toPublicApiModel(): ConnectorDefinitionResponse {
  return ConnectorDefinitionResponse(
    id = this.destinationDefinitionId.toString(),
    name = this.name,
    connectorDefinitionType = ConnectorType.DESTINATION,
    version = this.dockerImageTag,
  )
}

fun DestinationDefinitionReadList.toPublicApiModel(): ConnectorDefinitionsResponse {
  return ConnectorDefinitionsResponse(
    data = this.destinationDefinitions.map { it.toPublicApiModel() },
  )
}
