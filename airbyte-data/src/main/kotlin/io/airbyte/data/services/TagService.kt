/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.Tag
import java.util.UUID

/**
 * A service that manages tags
 */
interface TagService {
  fun getTag(
    tagId: UUID,
    workspaceId: UUID,
  ): Tag

  fun getTagById(tagId: UUID): Tag

  fun getTagsByWorkspaceIds(workspaceIds: List<UUID>): List<Tag>

  fun updateTag(tag: Tag): Tag

  fun deleteTag(tagId: UUID)

  fun createTag(tag: Tag): Tag
}
