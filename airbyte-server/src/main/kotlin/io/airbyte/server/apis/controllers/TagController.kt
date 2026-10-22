/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.models.TagCreateRequestBody
import io.airbyte.api.server.generated.models.TagDeleteRequestBody
import io.airbyte.api.server.generated.models.TagListRequestBody
import io.airbyte.api.server.generated.models.TagUpdateRequestBody
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
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
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  fun listTags(
    @Body tagListRequestBody: TagListRequestBody,
  ) = tagHandler.getTagsForWorkspaces(listOf(tagListRequestBody.workspaceId))

  @Post("/create")
  @RequiresIntent(Intent.CreateOrEditConnection)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  fun createTag(
    @Body tagCreateRequestyBody: TagCreateRequestBody,
  ) = tagHandler.createTag(tagCreateRequestyBody)

  @Post("/update")
  @RequiresIntent(Intent.CreateOrEditConnection)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  fun updateTag(
    @Body tagUpdateRequestBody: TagUpdateRequestBody,
  ) = tagHandler.updateTag(tagUpdateRequestBody)

  @Post("/delete")
  @RequiresIntent(Intent.CreateOrEditConnection)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  fun deleteTag(
    @Body tagDeleteRequestBody: TagDeleteRequestBody,
  ) = tagHandler.deleteTag(tagDeleteRequestBody)
}
