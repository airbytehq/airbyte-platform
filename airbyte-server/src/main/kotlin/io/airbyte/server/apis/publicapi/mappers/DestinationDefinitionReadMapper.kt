/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.DestinationDefinitionRead
import io.airbyte.api.model.generated.DestinationDefinitionReadList
import io.airbyte.publicApi.server.generated.models.ConnectorDefinitionResponse
import io.airbyte.publicApi.server.generated.models.ConnectorDefinitionsResponse
import io.airbyte.publicApi.server.generated.models.ConnectorType

fun DestinationDefinitionRead.toPublicApiModel(): ConnectorDefinitionResponse =
  ConnectorDefinitionResponse(
    id = this.destinationDefinitionId.toString(),
    name = this.name,
    connectorDefinitionType = ConnectorType.DESTINATION,
    version = this.dockerImageTag,
  )

fun DestinationDefinitionReadList.toPublicApiModel(): ConnectorDefinitionsResponse =
  ConnectorDefinitionsResponse(
    data = this.destinationDefinitions.map { it.toPublicApiModel() },
  )
