/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.publicApi.server.generated.models.GroupMemberResponse
import io.airbyte.publicApi.server.generated.models.GroupMembersResponse

/**
 * Mappers that help convert a list of GroupMemberResponse to GroupMembersResponse for the public API.
 */
object GroupMembersResponseMapper {
  /**
   * Converts a list of GroupMemberResponse objects to a GroupMembersResponse.
   *
   * @param members List of GroupMemberResponse objects
   * @param next Optional link to the next page
   * @param previous Optional link to the previous page
   * @return GroupMembersResponse containing the list of members
   */
  fun from(
    members: List<GroupMemberResponse>,
    next: String? = null,
    previous: String? = null,
  ): GroupMembersResponse =
    GroupMembersResponse(
      `data` = members,
      next = next,
      previous = previous,
    )
}
