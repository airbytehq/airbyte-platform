/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.models.TagCreateRequestBody
import io.airbyte.api.server.generated.models.TagDeleteRequestBody
import io.airbyte.api.server.generated.models.TagListRequestBody
import io.airbyte.api.server.generated.models.TagUpdateRequestBody
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.auth.TagReadRule
import io.airbyte.server.auth.TagWriteRule
import io.airbyte.server.handlers.TagHandler
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/api/v1/tags")
class TagController(
  private val tagHandler: TagHandler,
) {
  @Post("/list")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = TagReadRule::class,
  )
  fun listTags(
    @Body tagListRequestBody: TagListRequestBody,
  ) = tagHandler.getTagsForWorkspaces(listOf(tagListRequestBody.workspaceId))

  @Post("/create")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = TagWriteRule::class,
  )
  fun createTag(
    @Body tagCreateRequestyBody: TagCreateRequestBody,
  ) = tagHandler.createTag(tagCreateRequestyBody)

  @Post("/update")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = TagWriteRule::class,
  )
  fun updateTag(
    @Body tagUpdateRequestBody: TagUpdateRequestBody,
  ) = tagHandler.updateTag(tagUpdateRequestBody)

  @Post("/delete")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = TagWriteRule::class,
  )
  fun deleteTag(
    @Body tagDeleteRequestBody: TagDeleteRequestBody,
  ) = tagHandler.deleteTag(tagDeleteRequestBody)
}
