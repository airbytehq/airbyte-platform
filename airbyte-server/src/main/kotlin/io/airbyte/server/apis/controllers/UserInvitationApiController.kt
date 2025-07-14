/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.UserInvitationApi
import io.airbyte.api.model.generated.InviteCodeRequestBody
import io.airbyte.api.model.generated.UserInvitationCreateRequestBody
import io.airbyte.api.model.generated.UserInvitationCreateResponse
import io.airbyte.api.model.generated.UserInvitationListRequestBody
import io.airbyte.api.model.generated.UserInvitationRead
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.errors.OperationNotAllowedException
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.server.apis.execute
import io.airbyte.server.handlers.UserInvitationHandler
import io.airbyte.server.helpers.UserInvitationAuthorizationHelper
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.Post
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.Path
import jakarta.ws.rs.PathParam
import java.util.concurrent.Callable

@Controller("/api/v1/user_invitations")
@Secured(SecurityRule.IS_AUTHENTICATED)
class UserInvitationApiController(
  private val userInvitationHandler: UserInvitationHandler,
  private val currentUserService: CurrentUserService,
  private val userInvitationAuthorizationHelper: UserInvitationAuthorizationHelper,
) : UserInvitationApi {
  @Get
  @Path("/by_code/{inviteCode}")
  override fun getUserInvitation(
    @PathParam("inviteCode") inviteCode: String,
  ): UserInvitationRead? =
    execute(
      Callable {
        val currentUser = currentUserService.getCurrentUser()
        userInvitationHandler.getByInviteCode(inviteCode, currentUser)
      },
    )

  @Post
  @Path("/list_pending")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  override fun listPendingInvitations(
    @Body invitationListRequestBody: UserInvitationListRequestBody,
  ): List<io.airbyte.api.model.generated.UserInvitationRead> = userInvitationHandler.getPendingInvitations(invitationListRequestBody)

  @Secured(AuthRoleConstants.WORKSPACE_ADMIN, AuthRoleConstants.ORGANIZATION_ADMIN)
  override fun createUserInvitation(
    @Body invitationCreateRequestBody: UserInvitationCreateRequestBody?,
  ): UserInvitationCreateResponse? =
    execute {
      val currentUser = currentUserService.getCurrentUser()
      userInvitationHandler.createInvitationOrPermission(invitationCreateRequestBody!!, currentUser)
    }

  override fun acceptUserInvitation(
    @Body inviteCodeRequestBody: InviteCodeRequestBody,
  ): io.airbyte.api.model.generated.UserInvitationRead? =
    execute {
      val currentUser = currentUserService.getCurrentUser()
      userInvitationHandler.accept(inviteCodeRequestBody, currentUser)
    }

  override fun declineUserInvitation(
    @Body inviteCodeRequestBody: InviteCodeRequestBody?,
  ): io.airbyte.api.model.generated.UserInvitationRead? =
    execute<UserInvitationRead?> {
      // TODO only the invitee can decline the invitation
      throw RuntimeException("Not yet implemented")
    }

  override fun cancelUserInvitation(
    @Body inviteCodeRequestBody: InviteCodeRequestBody,
  ): io.airbyte.api.model.generated.UserInvitationRead? {
    // note: this endpoint is accessible to all authenticated users, but `authorizeInvitationAdmin`
    // throws a 403 if a non-admin user of the invitation's scope tries to cancel it.
    return execute {
      authorizeInvitationAdmin(inviteCodeRequestBody.inviteCode)
      userInvitationHandler.cancel(inviteCodeRequestBody)
    }
  }

  private fun authorizeInvitationAdmin(inviteCode: String) {
    val currentUserId = currentUserService.getCurrentUser().userId
    try {
      userInvitationAuthorizationHelper.authorizeInvitationAdmin(inviteCode, currentUserId)
    } catch (e: Exception) {
      throw OperationNotAllowedException("Admin authorization failed for invite code: $inviteCode and user id: $currentUserId", e)
    }
  }
}
