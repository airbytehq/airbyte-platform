/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.SourceRead
import io.airbyte.publicApi.server.generated.models.SourceResponse
import io.airbyte.server.apis.publicapi.helpers.toPublic

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
  fun from(sourceRead: SourceRead): SourceResponse =
    SourceResponse(
      sourceId = sourceRead.sourceId.toString(),
      name = sourceRead.name,
      sourceType = DEFINITION_ID_TO_SOURCE_NAME.getOrDefault(sourceRead.sourceDefinitionId, ""),
      workspaceId = sourceRead.workspaceId.toString(),
      configuration = sourceRead.connectionConfiguration,
      definitionId = sourceRead.sourceDefinitionId.toString(),
      createdAt = sourceRead.createdAt,
      resourceAllocation = sourceRead.resourceAllocation?.toPublic(),
    )
}
