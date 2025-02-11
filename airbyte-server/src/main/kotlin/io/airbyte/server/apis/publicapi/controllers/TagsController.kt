/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.model.generated.PermissionType
import io.airbyte.commons.server.authorization.ApiAuthorizationHelper
import io.airbyte.commons.server.authorization.Scope
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
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.core.Response
import java.util.UUID

@Controller(API_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class TagsController(
  private val apiAuthorizationHelper: ApiAuthorizationHelper,
  private val currentUserService: CurrentUserService,
  private val tagService: TagService,
  private val trackingHelper: TrackingHelper,
) : PublicTagsApi {
  override fun publicCreateTag(tagCreateRequest: TagCreateRequest): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermission(
      tagCreateRequest.workspaceId.toString(),
      Scope.WORKSPACE,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )

    val tag =
      trackingHelper.callWithTracker({
        tagService.createTag(tagCreateRequest)
      }, TAGS_PATH, POST, userId)

    return Response
      .status(Response.Status.OK.statusCode)
      .entity(tag)
      .build()
  }

  override fun publicDeleteTag(tagId: UUID): Response {
    // We need to manually fetch the workspaceId here because there is no Tag scope in the ApiAuthorizationHelper
    val workspaceId: UUID = tagService.getTag(tagId).workspaceId
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermission(
      workspaceId.toString(),
      Scope.WORKSPACE,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )

    trackingHelper.callWithTracker({
      tagService.deleteTag(tagId, workspaceId)
    }, TAGS_PATH, GET, userId)

    return Response
      .status(Response.Status.NO_CONTENT)
      .build()
  }

  override fun publicGetTag(tagId: UUID): Response {
    // We need to manually fetch the workspaceId here because there is no Tag scope in the ApiAuthorizationHelper
    val workspaceId: UUID = tagService.getTag(tagId).workspaceId
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermission(
      workspaceId.toString(),
      Scope.WORKSPACE,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )

    val tag =
      trackingHelper.callWithTracker({
        tagService.getTag(tagId)
      }, TAGS_PATH, GET, userId)

    return Response
      .status(Response.Status.OK.statusCode)
      .entity(tag)
      .build()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicListTags(workspaceIds: List<UUID>?): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacesPermission(
      workspaceIds?.let { workspaceIds.map { it.toString() } } ?: emptyList(),
      Scope.WORKSPACES,
      userId,
      PermissionType.WORKSPACE_READER,
    )
    val tags =
      trackingHelper.callWithTracker({
        tagService.listTags(workspaceIds ?: emptyList())
      }, TAGS_PATH, GET, userId)

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

    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermission(
      workspaceId.toString(),
      Scope.WORKSPACE,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )

    val tag =
      trackingHelper.callWithTracker({
        tagService.updateTag(tagId, workspaceId, tagPatchRequest)
      }, TAGS_PATH, POST, userId)

    return Response
      .status(Response.Status.OK.statusCode)
      .entity(tag)
      .build()
  }
}
