/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.ConnectionReadList
import io.airbyte.publicApi.server.generated.models.ConnectionsResponse
import io.airbyte.server.apis.publicapi.constants.CONNECTIONS_PATH
import io.airbyte.server.apis.publicapi.constants.INCLUDE_DELETED
import io.airbyte.server.apis.publicapi.constants.WORKSPACE_IDS
import io.airbyte.server.apis.publicapi.helpers.removePublicApiPathPrefix
import java.util.UUID

/**
 * Mappers that help convert models from the config api to models from the public api.
 */
object ConnectionsResponseMapper {
  /**
   * Converts a ConnectionReadList object from the config api to a ConnectionsResponse object.
   *
   * @param connectionReadList Output of a connection list from config api
   * @param workspaceIds workspaceIds requested by the user, if empty assume all workspaces requested
   * @param includeDeleted did we include deleted workspaces or not?
   * @param limit Number of JobResponses to be outputted
   * @param offset Offset of the pagination
   * @param apiHost Host url e.g. api.airbyte.com
   * @return JobsResponse List of JobResponse along with a next and previous https requests
   */
  fun from(
    connectionReadList: ConnectionReadList,
    workspaceIds: List<UUID>,
    includeDeleted: Boolean,
    limit: Int,
    offset: Int,
    apiHost: String,
  ): ConnectionsResponse {
    val uriBuilder =
      PaginationMapper
        .getBuilder(apiHost, removePublicApiPathPrefix(CONNECTIONS_PATH))
        .queryParam(INCLUDE_DELETED, includeDeleted)

    if (workspaceIds.isNotEmpty()) {
      uriBuilder.queryParam(WORKSPACE_IDS, PaginationMapper.uuidListToQueryString(workspaceIds))
    }
    return ConnectionsResponse(
      next = PaginationMapper.getNextUrl(connectionReadList.connections, limit, offset, uriBuilder),
      previous = PaginationMapper.getPreviousUrl(limit, offset, uriBuilder),
      data =
        connectionReadList.connections.map { connectionRead ->
          ConnectionReadMapper.from(connectionRead, connectionRead.workspaceId)
        },
    )
  }
}
