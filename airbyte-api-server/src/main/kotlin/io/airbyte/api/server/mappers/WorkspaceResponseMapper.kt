/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.mappers

import io.airbyte.airbyte_api.model.generated.GeographyEnum
import io.airbyte.airbyte_api.model.generated.WorkspaceResponse
import io.airbyte.api.client.model.generated.WorkspaceRead

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
  fun from(workspaceRead: WorkspaceRead): WorkspaceResponse {
    val workspaceResponse = WorkspaceResponse()
    workspaceResponse.workspaceId = workspaceRead.workspaceId
    workspaceResponse.name = workspaceRead.name
    if (workspaceRead.defaultGeography != null) {
      workspaceResponse.dataResidency = GeographyEnum.fromValue(workspaceRead.defaultGeography!!.toString())
    }
    return workspaceResponse
  }
}
