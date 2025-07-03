/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.Tag
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.repositories.TagRepository
import io.airbyte.data.services.TagService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.micronaut.transaction.annotation.Transactional
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
open class TagServiceDataImpl(
  private val tagRepository: TagRepository,
) : TagService {
  override fun getTagsByWorkspaceIds(workspaceIds: List<UUID>): List<Tag> = tagRepository.findByWorkspaceIdIn(workspaceIds).map { it.toConfigModel() }

  override fun getTag(
    tagId: UUID,
    workspaceId: UUID,
  ): Tag = tagRepository.findByIdAndWorkspaceId(tagId, workspaceId).toConfigModel()

  override fun getTagById(tagId: UUID): Tag =
    tagRepository
      .findById(tagId)
      .orElseThrow { throw ConfigNotFoundException(ConfigNotFoundType.TAG, tagId) }
      .toConfigModel()

  @Transactional("config")
  override fun updateTag(tag: Tag): Tag {
    if (tagRepository.findByIdForUpdate(tag.tagId).workspaceId != tag.workspaceId) {
      throw IllegalArgumentException("Cannot update workspaceId of a tag")
    }
    return tagRepository.update(tag.toEntity()).toConfigModel()
  }

  override fun deleteTag(tagId: UUID) {
    tagRepository.deleteById(tagId)
  }

  override fun createTag(tag: Tag): Tag {
    if (tagRepository.countByWorkspaceId(tag.workspaceId) >= 100) {
      throw IllegalStateException("Maximum 100 tags can be created in a workspace")
    }
    return tagRepository.save(tag.toEntity()).toConfigModel()
  }
}
