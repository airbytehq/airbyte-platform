/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.TagCreateRequestBody
import io.airbyte.publicApi.server.generated.models.TagCreateRequest

object TagCreateMapper {
  fun from(tagCreateRequest: TagCreateRequest): TagCreateRequestBody {
    val tagCreateRequestBody = TagCreateRequestBody()
    tagCreateRequestBody.workspaceId = tagCreateRequest.workspaceId
    tagCreateRequestBody.name = tagCreateRequest.name
    tagCreateRequestBody.color = tagCreateRequest.color
    return tagCreateRequestBody
  }
}
