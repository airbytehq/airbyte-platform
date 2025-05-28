/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.AuthenticationId
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
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.core.Response
import java.util.UUID

@Controller(API_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class TagsController(
  private val roleResolver: RoleResolver,
  private val currentUserService: CurrentUserService,
  private val tagService: TagService,
  private val trackingHelper: TrackingHelper,
) : PublicTagsApi {
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR)
  override fun publicCreateTag(tagCreateRequest: TagCreateRequest): Response {
    val tag =
      trackingHelper.callWithTracker({
        tagService.createTag(tagCreateRequest)
      }, TAGS_PATH, POST, currentUserService.currentUser.userId)

    return Response
      .status(Response.Status.OK.statusCode)
      .entity(tag)
      .build()
  }

  override fun publicDeleteTag(tagId: UUID): Response {
    // We need to manually fetch the workspaceId here because there is no Tag scope in the ApiAuthorizationHelper
    val workspaceId: UUID = tagService.getTag(tagId).workspaceId

    roleResolver
      .Request()
      .withCurrentUser()
      .withRef(AuthenticationId.WORKSPACE_ID, workspaceId)
      .requireRole(AuthRoleConstants.WORKSPACE_EDITOR)

    trackingHelper.callWithTracker({
      tagService.deleteTag(tagId, workspaceId)
    }, TAGS_PATH, GET, currentUserService.currentUser.userId)

    return Response
      .status(Response.Status.NO_CONTENT)
      .build()
  }

  override fun publicGetTag(tagId: UUID): Response {
    // We need to manually fetch the workspaceId here because there is no Tag scope in the ApiAuthorizationHelper
    val workspaceId: UUID = tagService.getTag(tagId).workspaceId

    roleResolver
      .Request()
      .withCurrentUser()
      .withRef(AuthenticationId.WORKSPACE_ID, workspaceId)
      .requireRole(AuthRoleConstants.WORKSPACE_READER)

    val tag =
      trackingHelper.callWithTracker({
        tagService.getTag(tagId)
      }, TAGS_PATH, GET, currentUserService.currentUser.userId)

    return Response
      .status(Response.Status.OK.statusCode)
      .entity(tag)
      .build()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicListTags(workspaceIds: List<UUID>?): Response {
    // If workspace IDs were given, then verify the user has access to those workspaces.
    // If none were given, then the TagService will determine the workspaces for the current user.
    if (!workspaceIds.isNullOrEmpty()) {
      roleResolver
        .Request()
        .withCurrentUser()
        .withWorkspaces(workspaceIds)
        .requireRole(AuthRoleConstants.WORKSPACE_READER)
    }

    val tags =
      trackingHelper.callWithTracker({
        tagService.listTags(workspaceIds ?: emptyList())
      }, TAGS_PATH, GET, currentUserService.currentUser.userId)

    return Response
      .status(Response.Status.OK.statusCode)
      .entity(tags)
      .build()
  }

  override fun publicUpdateTag(
    tagId: UUID,
    tagPatchRequest: TagPatchRequest,
  ): Response {
    // We need to manually fetch the workspaceId here because there is no Tag scope in the ApiAuthorizationHelper
    val workspaceId: UUID = tagService.getTag(tagId).workspaceId

    roleResolver
      .Request()
      .withCurrentUser()
      .withRef(AuthenticationId.WORKSPACE_ID, workspaceId)
      .requireRole(AuthRoleConstants.WORKSPACE_EDITOR)

    val tag =
      trackingHelper.callWithTracker({
        tagService.updateTag(tagId, workspaceId, tagPatchRequest)
      }, TAGS_PATH, POST, currentUserService.currentUser.userId)

    return Response
      .status(Response.Status.OK.statusCode)
      .entity(tag)
      .build()
  }
}
