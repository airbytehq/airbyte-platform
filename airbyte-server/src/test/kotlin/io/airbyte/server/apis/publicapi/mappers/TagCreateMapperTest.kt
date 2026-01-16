/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.publicApi.server.generated.models.TagCreateRequest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class TagCreateMapperTest {
  @Test
  fun `Should convert a TagCreateRequest from the public api to a TagCreateRequestBody from the config api`() {
    val tagCreateRequest: TagCreateRequest =
      TagCreateRequest(
        name = "test",
        color = "#000000",
        workspaceId = UUID.randomUUID(),
      )

    val mapped = TagCreateMapper.from(tagCreateRequest)

    assertEquals(tagCreateRequest.name, mapped.name)
    assertEquals(tagCreateRequest.color, mapped.color)
    assertEquals(tagCreateRequest.workspaceId, mapped.workspaceId)
  }
}
