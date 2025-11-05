/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.config.GroupMember
import io.airbyte.publicApi.server.generated.models.GroupMemberResponse

/**
 * Converts a GroupMember domain model to a GroupMemberResponse for the public API.
 * Email and name must be present (should always be populated from read operations with JOIN).
 *
 * @return GroupMemberResponse for the public API
 * @throws IllegalStateException if email or name are null (indicates wrong mapper was used)
 */
fun GroupMember.toGroupMemberResponse(): GroupMemberResponse =
  GroupMemberResponse(
    memberId = this.id,
    groupId = this.groupId,
    userId = this.userId,
    userEmail = this.email ?: throw IllegalStateException("Email must be present for API response"),
    userName = this.name ?: throw IllegalStateException("Name must be present for API response"),
  )

/**
 * Converts a list of GroupMember domain models to a list of GroupMemberResponses.
 *
 * @return List of GroupMemberResponse for the public API
 */
fun List<GroupMember>.toGroupMemberResponses(): List<GroupMemberResponse> = this.map { it.toGroupMemberResponse() }
