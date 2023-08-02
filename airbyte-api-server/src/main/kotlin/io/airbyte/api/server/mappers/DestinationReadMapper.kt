/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.mappers

import io.airbyte.airbyte_api.model.generated.DestinationResponse
import io.airbyte.api.client.model.generated.DestinationRead

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
    val destinationResponse = DestinationResponse()
    destinationResponse.destinationId = destinationRead.destinationId
    destinationResponse.name = destinationRead.name
    destinationResponse.destinationType = DEFINITION_ID_TO_DESTINATION_NAME[destinationRead.destinationDefinitionId]
    destinationResponse.workspaceId = destinationRead.workspaceId
    destinationResponse.configuration = destinationRead.connectionConfiguration
    return destinationResponse
  }
}
