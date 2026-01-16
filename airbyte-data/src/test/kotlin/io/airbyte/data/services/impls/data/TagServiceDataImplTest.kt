/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.Tag
import io.airbyte.data.repositories.TagRepository
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import java.util.UUID

private val MOCK_WORKSPACE_ID = UUID.randomUUID()

private val MOCK_TAG_ONE =
  Tag().apply {
    workspaceId = MOCK_WORKSPACE_ID
    name = "Test tag"
    color = "3623e1"
  }

private val MOCK_TAG_TWO =
  Tag().apply {
    workspaceId = MOCK_WORKSPACE_ID
    name = "Second test tag"
    color = "3623e1"
  }

class TagServiceDataImplTest {
  private lateinit var tagServiceDataImpl: TagServiceDataImpl
  private val tagRepository: TagRepository = mockk()

  @BeforeEach
  fun setUp() {
    tagServiceDataImpl = TagServiceDataImpl(tagRepository)
  }

  @Test
  fun `getTagsByWorkspaceId returns tags when workspace id exists`() {
    val mockWorkspaceId = UUID.randomUUID()
    every { tagRepository.findByWorkspaceIdIn(listOf(mockWorkspaceId)) } returns listOf(MOCK_TAG_ONE.toEntity(), MOCK_TAG_TWO.toEntity())

    val result = tagServiceDataImpl.getTagsByWorkspaceIds(listOf(mockWorkspaceId))

    assertEquals(result.size, 2)
  }

  @Test
  fun `100 tags can be created in a workspace`() {
    every { tagRepository.countByWorkspaceId(MOCK_WORKSPACE_ID) } returns 99
    every { tagRepository.save(any()) } answers { firstArg() }

    assertDoesNotThrow {
      tagServiceDataImpl.createTag(MOCK_TAG_ONE)
    }
  }

  @Test
  fun `no more than 100 tags can be created in a workspace`() {
    every { tagRepository.countByWorkspaceId(MOCK_WORKSPACE_ID) } returns 100

    assertThrows<IllegalStateException> {
      tagServiceDataImpl.createTag(MOCK_TAG_ONE)
    }
  }

  @Test
  fun `workspaceId cannot be updated`() {
    val existingTag =
      Tag().apply {
        tagId = UUID.randomUUID()
        workspaceId = UUID.randomUUID()
        name = "Test tag"
        color = "3623e1"
      }

    every { tagRepository.findByIdForUpdate(existingTag.tagId) } returns existingTag.toEntity()

    val updatedTag =
      Tag().apply {
        tagId = existingTag.tagId
        workspaceId = UUID.randomUUID()
        name = existingTag.name
        color = existingTag.color
      }

    assertThrows<IllegalArgumentException> {
      tagServiceDataImpl.updateTag(updatedTag)
    }
  }
}
