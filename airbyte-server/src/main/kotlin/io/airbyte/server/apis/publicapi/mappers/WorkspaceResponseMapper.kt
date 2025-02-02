/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.WorkspaceRead
import io.airbyte.publicApi.server.generated.models.GeographyEnum
import io.airbyte.publicApi.server.generated.models.WorkspaceResponse

/**
 * Mappers that help convert models from the config api to models from the public api.
 */
object WorkspaceResponseMapper {
  /**
   * Converts a WorkspaceRead object from the config api to an object with just a WorkspaceId.
   *
   * @param workspaceRead Output of a workspace create/get from config api
   * @return WorkspaceResponse Response object which contains the workspace id
   */
  fun from(workspaceRead: WorkspaceRead): WorkspaceResponse =
    WorkspaceResponse(
      workspaceId = workspaceRead.workspaceId.toString(),
      name = workspaceRead.name,
      dataResidency =
        workspaceRead.defaultGeography?.let { defaultGeography ->
          GeographyEnum.valueOf(defaultGeography.toString().uppercase())
        } ?: GeographyEnum.AUTO,
    )
}
