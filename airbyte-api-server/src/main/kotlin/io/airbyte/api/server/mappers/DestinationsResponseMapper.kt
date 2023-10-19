/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.mappers

import io.airbyte.airbyte_api.model.generated.DestinationsResponse
import io.airbyte.api.client.model.generated.DestinationRead
import io.airbyte.api.client.model.generated.DestinationReadList
import io.airbyte.api.server.constants.DESTINATIONS_PATH
import io.airbyte.api.server.constants.INCLUDE_DELETED
import io.airbyte.api.server.constants.WORKSPACE_IDS
import java.util.UUID
import java.util.function.Function

/**
 * Maps config API DestinationReadLists to DestinationsResponse.
 */
object DestinationsResponseMapper {
  /**
   * Converts a SourceReadList object from the config api to a SourcesResponse object.
   *
   * @param destinationReadList Output of a destination list from config api
   * @param workspaceIds workspaceIds we wanted to list
   * @param includeDeleted did we include deleted workspaces or not?
   * @param limit Number of responses to be outputted
   * @param offset Offset of the pagination
   * @param apiHost Host url e.g. api.airbyte.com
   * @return DestinationsResponse List of DestinationResponse along with a next and previous https
   * requests
   */
  fun from(
    destinationReadList: DestinationReadList,
    workspaceIds: List<UUID>,
    includeDeleted: Boolean,
    limit: Int,
    offset: Int,
    apiHost: String,
  ): DestinationsResponse {
    val uriBuilder =
      PaginationMapper.getBuilder(apiHost, DESTINATIONS_PATH)
        .queryParam(WORKSPACE_IDS, PaginationMapper.uuidListToQueryString(workspaceIds))
        .queryParam(INCLUDE_DELETED, includeDeleted)
    val destinationsResponse = DestinationsResponse()
    destinationsResponse.setNext(PaginationMapper.getNextUrl(destinationReadList.destinations, limit, offset, uriBuilder))
    destinationsResponse.setPrevious(PaginationMapper.getPreviousUrl(limit, offset, uriBuilder))
    destinationsResponse.setData(
      destinationReadList.destinations.stream()
        .map(Function { obj: DestinationRead? -> DestinationReadMapper.from(obj!!) })
        .toList(),
    )
    return destinationsResponse
  }
}
