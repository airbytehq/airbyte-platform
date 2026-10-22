/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.server.generated.models.TagCreateRequestBody
import io.airbyte.publicApi.server.generated.models.TagCreateRequest

object TagCreateMapper {
  fun from(tagCreateRequest: TagCreateRequest): TagCreateRequestBody =
    TagCreateRequestBody(
      workspaceId = tagCreateRequest.workspaceId,
      name = tagCreateRequest.name,
      color = tagCreateRequest.color,
    )
}
