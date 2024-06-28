/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.SourceRead
import io.airbyte.publicApi.server.generated.models.SourceResponse

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
    return SourceResponse(
      sourceId = sourceRead.sourceId.toString(),
      name = sourceRead.name,
      sourceType = DEFINITION_ID_TO_SOURCE_NAME.getOrDefault(sourceRead.sourceDefinitionId, ""),
      workspaceId = sourceRead.workspaceId.toString(),
      configuration = sourceRead.connectionConfiguration,
    )
  }
}
