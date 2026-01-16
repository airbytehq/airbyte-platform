/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.TagCreateRequestBody
import io.airbyte.api.model.generated.TagDeleteRequestBody
import io.airbyte.api.model.generated.TagListRequestBody
import io.airbyte.api.model.generated.TagUpdateRequestBody
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.handlers.TagHandler
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn

@Controller("/api/v1/tags")
class TagController(
  private val tagHandler: TagHandler,
) {
  @Post("/list")
  @RequiresIntent(Intent.ViewConnection)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  fun listTags(
    @Body tagListRequestBody: TagListRequestBody,
  ) = tagHandler.getTagsForWorkspaces(listOf(tagListRequestBody.workspaceId))

  @Post("/create")
  @RequiresIntent(Intent.CreateOrEditConnection)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  fun createTag(
    @Body tagCreateRequestyBody: TagCreateRequestBody,
  ) = tagHandler.createTag(tagCreateRequestyBody)

  @Post("/update")
  @RequiresIntent(Intent.CreateOrEditConnection)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  fun updateTag(
    @Body tagUpdateRequestBody: TagUpdateRequestBody,
  ) = tagHandler.updateTag(tagUpdateRequestBody)

  @Post("/delete")
  @RequiresIntent(Intent.CreateOrEditConnection)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  fun deleteTag(
    @Body tagDeleteRequestBody: TagDeleteRequestBody,
  ) = tagHandler.deleteTag(tagDeleteRequestBody)
}
