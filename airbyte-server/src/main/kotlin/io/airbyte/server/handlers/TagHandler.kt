/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import io.airbyte.api.model.generated.Tag
import io.airbyte.api.model.generated.TagCreateRequestBody
import io.airbyte.api.model.generated.TagDeleteRequestBody
import io.airbyte.api.model.generated.TagUpdateRequestBody
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.TagService
import io.micronaut.data.exceptions.DataAccessException
import io.micronaut.data.exceptions.EmptyResultException
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class TagHandler(
  private val tagService: TagService,
) {
  fun getTagsForWorkspaces(workspaceIds: List<UUID>): List<Tag> =
    tagService.getTagsByWorkspaceIds(workspaceIds).map {
      Tag()
        .tagId(it.tagId)
        .workspaceId(it.workspaceId)
        .name(it.name)
        .color(it.color)
    }

  fun createTag(tagCreateRequestBody: TagCreateRequestBody): Tag {
    val tag =
      io.airbyte.config.Tag().apply {
        name = validateTagName(tagCreateRequestBody.name)
        color = tagCreateRequestBody.color
        workspaceId = tagCreateRequestBody.workspaceId
      }
    try {
      return tagService.createTag(tag).let {
        Tag()
          .tagId(it.tagId)
          .workspaceId(it.workspaceId)
          .name(it.name)
          .color(it.color)
      }
    } catch (e: DataAccessException) {
      handleDataAccessException(e)
    } catch (e: IllegalStateException) {
      if (e.message == "Maximum 100 tags can be created in a workspace") {
        throw io.airbyte.api.problems.throwable.generated
          .TagLimitForWorkspaceReachedProblem()
      } else {
        throw e
      }
    }
  }

  fun updateTag(tagUpdateRequestBody: TagUpdateRequestBody): Tag {
    checkTagExists(tagUpdateRequestBody.tagId, tagUpdateRequestBody.workspaceId)
    val updatedTag =
      io.airbyte.config
        .Tag()
        .apply {
          tagId = tagUpdateRequestBody.tagId
          workspaceId = tagUpdateRequestBody.workspaceId
          name = validateTagName(tagUpdateRequestBody.name)
          color = tagUpdateRequestBody.color
        }
    try {
      return tagService.updateTag(updatedTag).let {
        Tag()
          .tagId(it.tagId)
          .workspaceId(it.workspaceId)
          .name(it.name)
          .color(it.color)
      }
    } catch (e: DataAccessException) {
      handleDataAccessException(e)
    }
  }

  fun deleteTag(tagDeleteRequestBody: TagDeleteRequestBody) {
    checkTagExists(tagDeleteRequestBody.tagId, tagDeleteRequestBody.workspaceId)
    tagService.deleteTag(tagDeleteRequestBody.tagId)
  }

  fun getTag(tagId: UUID): Tag {
    val tag = tagService.getTagById(tagId)
    return Tag()
      .tagId(tag.tagId)
      .workspaceId(tag.workspaceId)
      .name(tag.name)
      .color(tag.color)
  }

  private fun checkTagExists(
    tagId: UUID,
    workspaceId: UUID,
  ) {
    try {
      tagService.getTag(tagId, workspaceId)
    } catch (e: EmptyResultException) {
      throw ConfigNotFoundException(ConfigNotFoundType.TAG, "Tag with id $tagId and workspaceId $workspaceId not found.")
    }
  }

  private fun validateTagName(name: String): String {
    if (name.length > 30) {
      throw io.airbyte.api.problems.throwable.generated
        .TagNameTooLongProblem()
    }
    return name
  }

  private fun handleDataAccessException(e: DataAccessException): Nothing {
    if (e.message?.contains("duplicate key value violates unique constraint") == true && e.message?.contains("tag_name_workspace_id_key") == true) {
      throw io.airbyte.api.problems.throwable.generated
        .TagAlreadyExistsProblem()
    }
    if ((e.message?.contains("violates check constraint") == true && e.message?.contains("valid_hex_color") == true) ||
      e.message?.contains("value too long for type character(6)") == true
    ) {
      throw io.airbyte.api.problems.throwable.generated
        .TagInvalidHexColorProblem()
    }
    throw e
  }
}
