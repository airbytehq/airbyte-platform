/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.server.generated.models.GroupResponse
import io.airbyte.api.server.generated.models.GroupsResponse

/**
 * Mappers that help convert a list of GroupResponse to GroupsResponse for the public API.
 */
object GroupsResponseMapper {
  /**
   * Converts a list of GroupResponse objects to a GroupsResponse.
   *
   * @param groups List of GroupResponse objects
   * @param next Optional link to the next page
   * @param previous Optional link to the previous page
   * @return GroupsResponse containing the list of groups
   */
  fun from(
    groups: List<GroupResponse>,
    next: String? = null,
    previous: String? = null,
  ): GroupsResponse =
    GroupsResponse(
      `data` = groups,
      next = next,
      previous = previous,
    )
}
