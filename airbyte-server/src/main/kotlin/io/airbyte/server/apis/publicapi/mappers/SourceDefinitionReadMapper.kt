package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.SourceDefinitionRead
import io.airbyte.api.model.generated.SourceDefinitionReadList
import io.airbyte.publicApi.server.generated.models.ConnectorDefinitionResponse
import io.airbyte.publicApi.server.generated.models.ConnectorDefinitionsResponse
import io.airbyte.publicApi.server.generated.models.ConnectorType

fun SourceDefinitionRead.toPublicApiModel(): ConnectorDefinitionResponse {
  return ConnectorDefinitionResponse(
    id = this.sourceDefinitionId.toString(),
    connectorDefinitionType = ConnectorType.SOURCE,
    name = this.name,
    version = this.dockerImageTag,
  )
}

fun SourceDefinitionReadList.toPublicApiModel(): ConnectorDefinitionsResponse {
  return ConnectorDefinitionsResponse(
    data = this.sourceDefinitions.map { it.toPublicApiModel() },
  )
}
