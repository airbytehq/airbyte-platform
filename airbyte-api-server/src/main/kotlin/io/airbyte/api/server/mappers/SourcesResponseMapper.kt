/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.mappers

import io.airbyte.airbyte_api.model.generated.SourcesResponse
import io.airbyte.api.client.model.generated.SourceRead
import io.airbyte.api.client.model.generated.SourceReadList
import io.airbyte.api.server.constants.INCLUDE_DELETED
import io.airbyte.api.server.constants.SOURCES_PATH
import io.airbyte.api.server.constants.WORKSPACE_IDS
import java.util.UUID

/**
 * Maps config API SourceReadList to SourcesResponse.
 */
object SourcesResponseMapper {
  /**
   * Converts a SourceReadList object from the config api to a SourcesResponse object.
   *
   * @param sourceReadList Output of a source list from config api
   * @param workspaceIds workspaceIds we wanted to list
   * @param includeDeleted did we include deleted workspaces or not?
   * @param limit Number of responses to be outputted
   * @param offset Offset of the pagination
   * @param apiHost Host url e.g. api.airbyte.com
   * @return SourcesResponse List of SourceResponse along with a next and previous https requests
   */
  fun from(
    sourceReadList: SourceReadList,
    workspaceIds: List<UUID>,
    includeDeleted: Boolean,
    limit: Int,
    offset: Int,
    apiHost: String,
  ): SourcesResponse {
    val uriBuilder =
      PaginationMapper.getBuilder(apiHost, SOURCES_PATH)
        .queryParam(WORKSPACE_IDS, PaginationMapper.uuidListToQueryString(workspaceIds))
        .queryParam(INCLUDE_DELETED, includeDeleted)
    val sourcesResponse = SourcesResponse()
    sourcesResponse.next = PaginationMapper.getNextUrl(sourceReadList.sources, limit, offset, uriBuilder)
    sourcesResponse.previous = PaginationMapper.getPreviousUrl(limit, offset, uriBuilder)
    sourcesResponse.data = sourceReadList.sources.map { obj: SourceRead? -> SourceReadMapper.from(obj!!) }
    return sourcesResponse
  }
}
