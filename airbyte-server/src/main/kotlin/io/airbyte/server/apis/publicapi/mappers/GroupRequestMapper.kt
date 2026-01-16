/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.config.Group
import io.airbyte.domain.models.GroupId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.publicApi.server.generated.models.GroupCreateRequest
import io.airbyte.publicApi.server.generated.models.GroupUpdateRequest
import java.time.OffsetDateTime
import java.util.UUID

/**
 * Converts a GroupCreateRequest to a Group domain model.
 *
 * @return Group domain model
 */
fun GroupCreateRequest.toGroupDomainModel(): Group =
  Group(
    groupId = GroupId(UUID.randomUUID()),
    name = this.name,
    description = this.description,
    organizationId = OrganizationId(this.organizationId),
    memberCount = null,
    createdAt = OffsetDateTime.now(),
    updatedAt = OffsetDateTime.now(),
  )

/**
 * Applies updates from a GroupUpdateRequest to an existing Group domain model.
 *
 * @param existingGroup The existing Group to update
 * @return Updated Group domain model
 */
fun GroupUpdateRequest.applyToGroupDomainModel(existingGroup: Group): Group =
  existingGroup.copy(
    name = this.name ?: existingGroup.name,
    description = this.description ?: existingGroup.description,
    updatedAt = OffsetDateTime.now(),
  )
