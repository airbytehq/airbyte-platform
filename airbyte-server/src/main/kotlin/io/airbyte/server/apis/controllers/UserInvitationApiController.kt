/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.UserInvitationApi
import io.airbyte.api.server.generated.models.InviteCodeRequestBody
import io.airbyte.api.server.generated.models.UserInvitationAdminRead
import io.airbyte.api.server.generated.models.UserInvitationCancelRequestBody
import io.airbyte.api.server.generated.models.UserInvitationCreateRequestBody
import io.airbyte.api.server.generated.models.UserInvitationCreateResponse
import io.airbyte.api.server.generated.models.UserInvitationListRequestBody
import io.airbyte.api.server.generated.models.UserInvitationRead
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.server.errors.BadRequestException
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.server.apis.execute
import io.airbyte.server.auth.AuthenticatedMetadataRule
import io.airbyte.server.auth.InvitationAcceptRule
import io.airbyte.server.auth.InvitationCancelRule
import io.airbyte.server.auth.InvitationReadRule
import io.airbyte.server.auth.InvitationScopeAdminRule
import io.airbyte.server.auth.InvitationScopeReadRule
import io.airbyte.server.handlers.UserInvitationHandler
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.PathParam
import java.util.concurrent.Callable

@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
class UserInvitationApiController(
  private val userInvitationHandler: UserInvitationHandler,
  private val currentUserService: CurrentUserService,
) : UserInvitationApi {
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = InvitationReadRule::class,
  )
  override fun getUserInvitation(
    @PathParam("inviteCode") inviteCode: String,
  ): UserInvitationRead =
    execute(
      Callable {
        val currentUser = currentUserService.getCurrentUser()
        userInvitationHandler.getByInviteCode(inviteCode, currentUser)
      },
    )

  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = InvitationScopeReadRule::class,
  )
  override fun listPendingInvitations(
    @Body invitationListRequestBody: UserInvitationListRequestBody?,
  ): List<UserInvitationAdminRead> = userInvitationHandler.getPendingInvitations(invitationListRequestBody)

  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = InvitationScopeAdminRule::class,
  )
  override fun createUserInvitation(
    @Body invitationCreateRequestBody: UserInvitationCreateRequestBody?,
  ): UserInvitationCreateResponse =
    execute {
      val currentUser = currentUserService.getCurrentUser()
      userInvitationHandler.createInvitationOrPermission(invitationCreateRequestBody!!, currentUser)
    }

  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = InvitationAcceptRule::class,
  )
  override fun acceptUserInvitation(
    @Body inviteCodeRequestBody: InviteCodeRequestBody?,
  ): UserInvitationRead =
    execute {
      val currentUser = currentUserService.getCurrentUser()
      userInvitationHandler.accept(inviteCodeRequestBody!!, currentUser)
    }

  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = AuthenticatedMetadataRule::class,
  )
  override fun declineUserInvitation(
    @Body inviteCodeRequestBody: InviteCodeRequestBody?,
  ): UserInvitationRead =
    execute<UserInvitationRead> {
      // TODO only the invitee can decline the invitation
      throw RuntimeException("Not yet implemented")
    }

  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = InvitationCancelRule::class,
  )
  override fun cancelUserInvitation(
    @Body invitationRequestBody: UserInvitationCancelRequestBody?,
  ): UserInvitationAdminRead =
    execute {
      val requestBody = invitationRequestBody ?: throw BadRequestException("Request body is required.")
      userInvitationHandler.cancel(requestBody)
    }
}
