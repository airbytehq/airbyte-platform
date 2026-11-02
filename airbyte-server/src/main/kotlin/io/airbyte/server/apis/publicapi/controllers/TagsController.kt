/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.publicApi.server.generated.apis.PublicTagsApi
import io.airbyte.publicApi.server.generated.models.TagCreateRequest
import io.airbyte.publicApi.server.generated.models.TagPatchRequest
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.API_PATH
import io.airbyte.server.apis.publicapi.constants.GET
import io.airbyte.server.apis.publicapi.constants.POST
import io.airbyte.server.apis.publicapi.constants.TAGS_PATH
import io.airbyte.server.apis.publicapi.services.TagService
import io.airbyte.server.auth.PublicTagCreateRule
import io.airbyte.server.auth.PublicTagReadRule
import io.airbyte.server.auth.PublicTagWriteRule
import io.airbyte.server.auth.PublicWorkspaceListRule
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.core.Response
import java.util.UUID

@Controller(API_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
open class TagsController(
  private val currentUserService: CurrentUserService,
  private val tagService: TagService,
  private val trackingHelper: TrackingHelper,
) : PublicTagsApi {
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = PublicTagCreateRule::class,
  )
  override fun publicCreateTag(tagCreateRequest: TagCreateRequest): Response {
    val tag =
      trackingHelper.callWithTracker({
        tagService.createTag(tagCreateRequest)
      }, TAGS_PATH, POST, currentUserService.getCurrentUser().userId)

    return Response
      .status(Response.Status.OK.statusCode)
      .entity(tag)
      .build()
  }

  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = PublicTagWriteRule::class,
  )
  override fun publicDeleteTag(tagId: UUID): Response {
    val workspaceId: UUID = tagService.getTag(tagId).workspaceId

    trackingHelper.callWithTracker({
      tagService.deleteTag(tagId, workspaceId)
    }, TAGS_PATH, GET, currentUserService.getCurrentUser().userId)

    return Response
      .status(Response.Status.NO_CONTENT)
      .build()
  }

  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = PublicTagReadRule::class,
  )
  override fun publicGetTag(tagId: UUID): Response {
    val tag =
      trackingHelper.callWithTracker({
        tagService.getTag(tagId)
      }, TAGS_PATH, GET, currentUserService.getCurrentUser().userId)

    return Response
      .status(Response.Status.OK.statusCode)
      .entity(tag)
      .build()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = PublicWorkspaceListRule::class,
  )
  override fun publicListTags(workspaceIds: List<UUID>?): Response {
    // If none were given, then the TagService will determine the workspaces for the current user.

    val tags =
      trackingHelper.callWithTracker({
        tagService.listTags(workspaceIds ?: emptyList())
      }, TAGS_PATH, GET, currentUserService.getCurrentUser().userId)

    return Response
      .status(Response.Status.OK.statusCode)
      .entity(tags)
      .build()
  }

  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = PublicTagWriteRule::class,
  )
  override fun publicUpdateTag(
    tagId: UUID,
    tagPatchRequest: TagPatchRequest,
  ): Response {
    val workspaceId: UUID = tagService.getTag(tagId).workspaceId

    val tag =
      trackingHelper.callWithTracker({
        tagService.updateTag(tagId, workspaceId, tagPatchRequest)
      }, TAGS_PATH, POST, currentUserService.getCurrentUser().userId)

    return Response
      .status(Response.Status.OK.statusCode)
      .entity(tag)
      .build()
  }
}
