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

  fun getTagsByWorkspaceId(workspaceId: UUID): List<Tag>

  fun updateTag(tag: Tag): Tag

  fun deleteTag(tagId: UUID)

  fun createTag(tag: Tag): Tag
}
