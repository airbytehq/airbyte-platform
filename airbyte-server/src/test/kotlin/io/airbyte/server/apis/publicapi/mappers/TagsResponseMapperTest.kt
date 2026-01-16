/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.Tag
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class TagsResponseMapperTest {
  @Test
  fun `Should convert a list of config api Tags to a TagsResponse for the public api`() {
    val tagList =
      listOf(
        Tag().apply {
          tagId = UUID.randomUUID()
          name = "test"
          color = "#000000"
          workspaceId = UUID.randomUUID()
        },
        Tag().apply {
          tagId = UUID.randomUUID()
          name = "test2"
          color = "#000001"
          workspaceId = UUID.randomUUID()
        },
      )

    val mapped = TagsResponseMapper.from(tagList)

    assertEquals(tagList.size, mapped.data.size)
    tagList.forEachIndexed { index, tag ->
      assertEquals(tag.tagId, mapped.data[index].tagId)
      assertEquals(tag.name, mapped.data[index].name)
      assertEquals(tag.color, mapped.data[index].color)
      assertEquals(tag.workspaceId, mapped.data[index].workspaceId)
    }
  }
}
