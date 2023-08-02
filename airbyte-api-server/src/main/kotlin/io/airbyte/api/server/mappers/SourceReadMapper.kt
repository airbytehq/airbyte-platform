/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.mappers

import io.airbyte.airbyte_api.model.generated.SourceResponse
import io.airbyte.api.client.model.generated.SourceRead

/**
 * Mappers that help convert models from the config api to models from the public api.
 */
object SourceReadMapper {
  /**
   * Converts a SourceRead object from the config api to a SourceResponse.
   *
   * @param sourceRead Output of a source create/get from config api
   * @return SourceResponse Response object with source details
   */
  fun from(sourceRead: SourceRead): SourceResponse {
    val sourceResponse = SourceResponse()
    sourceResponse.sourceId = sourceRead.sourceId
    sourceResponse.name = sourceRead.name
    sourceResponse.sourceType = DEFINITION_ID_TO_SOURCE_NAME.get(sourceRead.sourceDefinitionId)
    sourceResponse.workspaceId = sourceRead.workspaceId
    sourceResponse.configuration = sourceRead.connectionConfiguration
    return sourceResponse
  }
}
