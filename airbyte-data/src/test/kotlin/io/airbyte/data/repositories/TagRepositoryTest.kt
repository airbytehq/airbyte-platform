/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Tag
import io.airbyte.db.instance.configs.jooq.generated.Keys
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.tuple
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.util.UUID

@MicronautTest
class TagRepositoryTest : AbstractConfigRepositoryTest() {
  companion object {
    @BeforeAll
    @JvmStatic
    fun setup() {
      // so we don't have to deal with making an organization and workspace
      jooqDslContext.alterTable(Tables.TAG).dropForeignKey(Keys.TAG__TAG_WORKSPACE_ID_FKEY.constraint()).execute()
    }
  }

  @Test
  fun `count tags by workspace id`() {
    val firstWorkspaceId = UUID.randomUUID()
    tagRepository.save(Tag(workspaceId = firstWorkspaceId, name = "Test Tag", color = "3623e1"))
    tagRepository.save(Tag(workspaceId = firstWorkspaceId, name = "Test Tag 2", color = "ff0000"))

    val secondWorkspaceId = UUID.randomUUID()
    tagRepository.save(Tag(workspaceId = secondWorkspaceId, name = "Test Tag 3", color = "ff0000"))

    val countFirstWorkspace = tagRepository.countByWorkspaceId(firstWorkspaceId)
    val countSecondWorkspace = tagRepository.countByWorkspaceId(secondWorkspaceId)
    assertEquals(2, countFirstWorkspace)
    assertEquals(1, countSecondWorkspace)
  }

  @Test
  fun `create and retrieve tag`() {
    val tag = Tag(workspaceId = UUID.randomUUID(), name = "Test Tag", color = "3623e1")
    val savedTag = tagRepository.save(tag)

    val retrievedTag = tagRepository.findById(savedTag.id)
    assertTrue(retrievedTag.isPresent)
    assertThat(retrievedTag.get())
      .usingRecursiveComparison()
      .ignoringFields("createdAt", "updatedAt")
      .isEqualTo(tag)
  }

  @Test
  fun `update tag`() {
    val updatedName = "Updated tag name"
    val updatedColor = "ff0000"
    val tag = Tag(workspaceId = UUID.randomUUID(), name = "Test Tag", color = "3623e1")
    val savedTag = tagRepository.save(tag)

    tag.name = updatedName
    tag.color = updatedColor

    tagRepository.update(tag)

    val retrievedTag = tagRepository.findById(savedTag.id)
    assertTrue(retrievedTag.isPresent)
    assertEquals(updatedName, retrievedTag.get().name)
    assertEquals(updatedColor, retrievedTag.get().color)
  }

  @Test
  fun `find tags by workspace id`() {
    val workspaceId = UUID.randomUUID()
    val otherWorkspaceId = UUID.randomUUID()
    val tag1 = Tag(workspaceId = workspaceId, name = "Tag 1", color = "3623e1")
    val tag2 = Tag(workspaceId = workspaceId, name = "Tag 2", color = "ff0000")
    val tag3 = Tag(workspaceId = otherWorkspaceId, name = "Tag 3", color = "ffffff")
    tagRepository.save(tag1)
    tagRepository.save(tag2)
    tagRepository.save(tag3)

    val retrievedTags = tagRepository.findByWorkspaceIdIn(listOf(workspaceId))

    assertEquals(2, retrievedTags.size)
    assertThat(retrievedTags)
      .extracting("name", "color")
      .containsExactlyInAnyOrder(
        tuple("Tag 1", "3623e1"),
        tuple("Tag 2", "ff0000"),
      )
  }

  @Test
  fun `delete tag`() {
    val tag = Tag(workspaceId = UUID.randomUUID(), name = "Test Tag", color = "3623e1")
    val savedTag = tagRepository.save(tag)

    tagRepository.delete(tag)

    assertTrue(tagRepository.findById(savedTag.id).isEmpty)
  }
}
