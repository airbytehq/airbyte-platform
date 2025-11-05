/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.config.Group
import io.airbyte.publicApi.server.generated.models.GroupResponse

/**
 * Converts a Group domain model to a GroupResponse for the public API.
 *
 * @return GroupResponse for the public API
 */
fun Group.toGroupResponse(): GroupResponse =
  GroupResponse(
    groupId = this.groupId.value,
    name = this.name,
    description = this.description,
    organizationId = this.organizationId.value,
    memberCount = this.memberCount ?: 0,
  )
