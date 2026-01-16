/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.Tag
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class TagResponseMapperTest {
  @Test
  fun `Should map a Tag from the config api to a TagResponse in the public api`() {
    val tag =
      Tag().apply {
        tagId = UUID.randomUUID()
        name = "test"
        color = "#000000"
        workspaceId = UUID.randomUUID()
      }

    val mapped = TagResponseMapper.from(tag)

    assertEquals(tag.tagId, mapped.tagId)
    assertEquals(tag.name, mapped.name)
    assertEquals(tag.color, mapped.color)
    assertEquals(tag.workspaceId, mapped.workspaceId)
  }
}
