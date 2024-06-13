/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.DestinationRead
import io.airbyte.publicApi.server.generated.models.DestinationResponse

/**
 * Mappers that help convert models from the config api to models from the public api.
 */
object DestinationReadMapper {
  /**
   * Converts a DestinationRead object from the config api to a DestinationResponse.
   *
   * @param destinationRead Output of a destination create/get from config api
   * @return DestinationResponse Response object with destination details
   */
  fun from(destinationRead: DestinationRead): DestinationResponse {
    return DestinationResponse(
      destinationId = destinationRead.destinationId.toString(),
      name = destinationRead.name,
      destinationType = DEFINITION_ID_TO_DESTINATION_NAME.getOrDefault(destinationRead.destinationDefinitionId, ""),
      workspaceId = destinationRead.workspaceId.toString(),
      configuration = destinationRead.connectionConfiguration,
    )
  }
}
