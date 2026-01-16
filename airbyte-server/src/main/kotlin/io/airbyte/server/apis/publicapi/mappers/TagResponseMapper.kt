/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.Tag
import io.airbyte.publicApi.server.generated.models.TagResponse

object TagResponseMapper {
  fun from(tag: Tag): TagResponse =
    TagResponse(
      tagId = tag.tagId,
      name = tag.name,
      color = tag.color,
      workspaceId = tag.workspaceId,
    )
}
