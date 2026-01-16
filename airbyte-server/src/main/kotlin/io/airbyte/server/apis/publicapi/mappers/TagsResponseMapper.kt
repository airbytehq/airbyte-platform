/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.Tag
import io.airbyte.publicApi.server.generated.models.TagResponse
import io.airbyte.publicApi.server.generated.models.TagsResponse

object TagsResponseMapper {
  fun from(tagList: List<Tag>): TagsResponse =
    TagsResponse(
      data =
        tagList.map {
          TagResponse(
            tagId = it.tagId,
            name = it.name,
            color = it.color,
            workspaceId = it.workspaceId,
          )
        },
    )
}
