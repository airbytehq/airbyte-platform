/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.mappers

import io.airbyte.airbyte_api.model.generated.ConnectionsResponse
import io.airbyte.api.client.model.generated.ConnectionRead
import io.airbyte.api.client.model.generated.ConnectionReadList
import io.airbyte.api.server.constants.CONNECTIONS_PATH
import io.airbyte.api.server.constants.INCLUDE_DELETED
import io.airbyte.api.server.constants.WORKSPACE_IDS
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
      PaginationMapper.getBuilder(apiHost, CONNECTIONS_PATH)
        .queryParam(WORKSPACE_IDS, PaginationMapper.uuidListToQueryString(workspaceIds))
        .queryParam(INCLUDE_DELETED, includeDeleted)
    val connectionsResponse = ConnectionsResponse()
    connectionsResponse.setNext(PaginationMapper.getNextUrl(connectionReadList.connections, limit, offset, uriBuilder))
    connectionsResponse.setPrevious(PaginationMapper.getPreviousUrl(limit, offset, uriBuilder))
    connectionsResponse.setData(
      connectionReadList.connections.map { connectionRead: ConnectionRead -> ConnectionReadMapper.from(connectionRead, connectionRead.workspaceId) },
    )
    return connectionsResponse
  }
}
