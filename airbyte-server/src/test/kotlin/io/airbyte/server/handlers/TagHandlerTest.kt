/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import io.airbyte.api.model.generated.TagCreateRequestBody
import io.airbyte.api.model.generated.TagDeleteRequestBody
import io.airbyte.api.model.generated.TagUpdateRequestBody
import io.airbyte.api.problems.throwable.generated.TagAlreadyExistsProblem
import io.airbyte.api.problems.throwable.generated.TagInvalidHexColorProblem
import io.airbyte.api.problems.throwable.generated.TagLimitForWorkspaceReachedProblem
import io.airbyte.api.problems.throwable.generated.TagNameTooLongProblem
import io.airbyte.config.Tag
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.TagService
import io.micronaut.data.exceptions.DataAccessException
import io.micronaut.data.exceptions.EmptyResultException
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class TagHandlerTest {
  companion object {
    private val tagService = mockk<TagService>()
    private val tagHandler = TagHandler(tagService)
    private val MOCK_WORKSPACE_ID = UUID.randomUUID()
    private val MOCK_TAG_ONE =
      Tag().apply {
        tagId = UUID.randomUUID()
        workspaceId = MOCK_WORKSPACE_ID
        name = "Test tag"
        color = "3623e1"
      }
    private val MOCK_TAG_TWO =
      Tag().apply {
        tagId = UUID.randomUUID()
        workspaceId = MOCK_WORKSPACE_ID
        name = "Second test tag"
        color = "3623e1"
      }
    private const val INVALID_HEX = "not a hexadecimal value"
    private const val TAG_NAME_CONSTRAINT_VIOLATION_MESSAGE = "duplicate key value violates unique constraint: tag_name_workspace_id_key"
    private const val HEX_COLOR_CONSTRAINT_VIOLATION_MESSAGE = "violates check constraint: valid_hex_color"

    @JvmStatic
    @BeforeAll
    fun setup() {
      every { tagService.updateTag(match { it.color == INVALID_HEX }) } throws DataAccessException(HEX_COLOR_CONSTRAINT_VIOLATION_MESSAGE)
      every { tagService.createTag(match { it.color == INVALID_HEX }) } throws DataAccessException(HEX_COLOR_CONSTRAINT_VIOLATION_MESSAGE)
    }
  }

  @Test
  fun `getTagsForWorkspace returns tags`() {
    every { tagService.getTagsByWorkspaceIds(listOf(MOCK_WORKSPACE_ID)) } returns listOf(MOCK_TAG_ONE, MOCK_TAG_TWO)
    val tags = tagHandler.getTagsForWorkspaces(listOf(MOCK_WORKSPACE_ID))

    assert(tags.size == 2)
    assert(tags[0].tagId == MOCK_TAG_ONE.tagId)
    assert(tags[0].name == MOCK_TAG_ONE.name)
    assert(tags[1].tagId == MOCK_TAG_TWO.tagId)
    assert(tags[1].name == MOCK_TAG_TWO.name)
  }

  @Test
  fun `createTag returns the tag`() {
    every { tagService.createTag(any()) } returns MOCK_TAG_ONE
    val tag = tagHandler.createTag(TagCreateRequestBody().workspaceId(MOCK_TAG_ONE.workspaceId).name(MOCK_TAG_ONE.name).color(MOCK_TAG_ONE.color))

    assert(tag.workspaceId == MOCK_TAG_ONE.workspaceId)
    assert(tag.name == MOCK_TAG_ONE.name)
    assert(tag.color == MOCK_TAG_ONE.color)
  }

  @Test
  fun `createTag returns a problem when the max number of tags in a workspace has been exceeded`() {
    every { tagService.createTag(any()) } throws IllegalStateException("Maximum 100 tags can be created in a workspace")
    val tagCreateRequestBody =
      TagCreateRequestBody().workspaceId(MOCK_TAG_ONE.workspaceId).name(MOCK_TAG_ONE.name).color(MOCK_TAG_ONE.color)

    assertThrows<TagLimitForWorkspaceReachedProblem> {
      tagHandler.createTag(tagCreateRequestBody)
    }
  }

  @Test
  fun `createTag with a long name returns a problem`() {
    val tagCreateRequestBody =
      TagCreateRequestBody().workspaceId(MOCK_TAG_ONE.workspaceId).name("a".repeat(101)).color(MOCK_TAG_ONE.color)

    assertThrows<TagNameTooLongProblem> {
      tagHandler.createTag(tagCreateRequestBody)
    }
  }

  @Test
  fun `createTag with a duplicate name returns a problem`() {
    every { tagService.createTag(any()) } throws DataAccessException(TAG_NAME_CONSTRAINT_VIOLATION_MESSAGE)
    val tagCreateRequestBody =
      TagCreateRequestBody().workspaceId(MOCK_TAG_ONE.workspaceId).name(MOCK_TAG_ONE.name).color(MOCK_TAG_ONE.color)

    assertThrows<TagAlreadyExistsProblem> {
      tagHandler.createTag(tagCreateRequestBody)
    }
  }

  @Test
  fun `createTag with an invalid hex returns a problem`() {
    every { tagService.createTag(any()) } throws DataAccessException(HEX_COLOR_CONSTRAINT_VIOLATION_MESSAGE)
    val tagCreateRequestBody =
      TagCreateRequestBody().workspaceId(MOCK_TAG_ONE.workspaceId).name(MOCK_TAG_ONE.name).color(INVALID_HEX)

    assertThrows<TagInvalidHexColorProblem> {
      tagHandler.createTag(tagCreateRequestBody)
    }
  }

  @Test
  fun `updateTag returns the tag`() {
    every { tagService.getTag(any(), any()) } returns MOCK_TAG_ONE
    every { tagService.updateTag(any()) } returns MOCK_TAG_ONE
    val updatedTag =
      tagHandler.updateTag(
        TagUpdateRequestBody()
          .tagId(MOCK_TAG_ONE.tagId)
          .workspaceId(MOCK_TAG_ONE.workspaceId)
          .name(MOCK_TAG_ONE.name)
          .color(MOCK_TAG_ONE.color),
      )

    assert(updatedTag.tagId == MOCK_TAG_ONE.tagId)
    assert(updatedTag.workspaceId == MOCK_TAG_ONE.workspaceId)
    assert(updatedTag.name == MOCK_TAG_ONE.name)
    assert(updatedTag.color == MOCK_TAG_ONE.color)
  }

  @Test
  fun `updateTag with a long name throws a problem`() {
    every { tagService.getTag(any(), any()) } returns MOCK_TAG_ONE
    val tagUpdateBody =
      TagUpdateRequestBody()
        .tagId(MOCK_TAG_ONE.tagId)
        .workspaceId(MOCK_TAG_ONE.workspaceId)
        .name(
          "a"
            .repeat(101),
        ).color(MOCK_TAG_ONE.color)

    assertThrows<TagNameTooLongProblem> {
      tagHandler.updateTag(tagUpdateBody)
    }
  }

  @Test
  fun `updateTag with a duplicate name`() {
    every { tagService.getTag(any(), any()) } returns MOCK_TAG_ONE
    every { tagService.updateTag(any()) } throws DataAccessException(TAG_NAME_CONSTRAINT_VIOLATION_MESSAGE)
    val tagUpdateBody =
      TagUpdateRequestBody()
        .tagId(MOCK_TAG_ONE.tagId)
        .workspaceId(MOCK_TAG_ONE.workspaceId)
        .name(MOCK_TAG_ONE.name)
        .color(MOCK_TAG_ONE.color)

    assertThrows<TagAlreadyExistsProblem> {
      tagHandler.updateTag(tagUpdateBody)
    }
  }

  @Test
  fun `updateTag with an invalid hex throws a problem`() {
    every { tagService.getTag(any(), any()) } returns MOCK_TAG_ONE
    every { tagService.updateTag(any()) } throws DataAccessException(HEX_COLOR_CONSTRAINT_VIOLATION_MESSAGE)
    val tagUpdateBody =
      TagUpdateRequestBody()
        .tagId(MOCK_TAG_ONE.tagId)
        .workspaceId(MOCK_TAG_ONE.workspaceId)
        .name(MOCK_TAG_ONE.name)
        .color(INVALID_HEX)

    assertThrows<TagInvalidHexColorProblem> {
      tagHandler.updateTag(tagUpdateBody)
    }
  }

  @Test
  fun `updateTag handles an error when tag cannot be found`() {
    every { tagService.getTag(any(), any()) } throws EmptyResultException()
    val tagUpdateBody =
      TagUpdateRequestBody()
        .tagId(MOCK_TAG_ONE.tagId)
        .workspaceId(MOCK_TAG_ONE.workspaceId)
        .name(MOCK_TAG_ONE.name)
        .color(MOCK_TAG_ONE.color)
    assertThrows<ConfigNotFoundException> {
      tagHandler.updateTag(tagUpdateBody)
    }
  }

  @Test
  fun `deleteTag handles an error when tag cannot be found `() {
    every { tagService.getTag(any(), any()) } throws EmptyResultException()
    val tagDeleteRequestBody =
      TagDeleteRequestBody().tagId(MOCK_TAG_ONE.tagId).workspaceId(MOCK_TAG_ONE.workspaceId)
    assertThrows<ConfigNotFoundException> {
      tagHandler.deleteTag(tagDeleteRequestBody)
    }
  }
}
