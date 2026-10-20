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
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.errors.BadRequestException
import io.airbyte.commons.server.errors.OperationNotAllowedException
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.server.apis.execute
import io.airbyte.server.handlers.UserInvitationHandler
import io.airbyte.server.helpers.UserInvitationAuthorizationHelper
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.PathParam
import java.util.UUID
import java.util.concurrent.Callable

@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
class UserInvitationApiController(
  private val userInvitationHandler: UserInvitationHandler,
  private val currentUserService: CurrentUserService,
  private val userInvitationAuthorizationHelper: UserInvitationAuthorizationHelper,
) : UserInvitationApi {
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
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

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun listPendingInvitations(
    @Body invitationListRequestBody: UserInvitationListRequestBody?,
  ): List<UserInvitationAdminRead> = userInvitationHandler.getPendingInvitations(invitationListRequestBody)

  @Secured(AuthRoleConstants.WORKSPACE_ADMIN, AuthRoleConstants.ORGANIZATION_ADMIN)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
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
    migration = RouteMigration.LEGACY,
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
    migration = RouteMigration.LEGACY,
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
    migration = RouteMigration.LEGACY,
  )
  override fun cancelUserInvitation(
    @Body invitationRequestBody: UserInvitationCancelRequestBody?,
  ): UserInvitationAdminRead {
    // note: this endpoint is accessible to all authenticated users, but `authorizeInvitationAdmin`
    // throws a 403 if a non-admin user of the invitation's scope tries to cancel it.
    return execute {
      val requestBody = invitationRequestBody ?: throw BadRequestException("Request body is required.")
      val invitationId = requestBody.id
      val inviteCode = requestBody.inviteCode
      when {
        invitationId != null -> authorizeInvitationAdmin(invitationId)
        inviteCode != null -> authorizeInvitationAdmin(inviteCode)
        else -> throw BadRequestException("Either id or inviteCode must be provided.")
      }
      userInvitationHandler.cancel(requestBody)
    }
  }

  private fun authorizeInvitationAdmin(id: UUID) {
    val currentUserId = currentUserService.getCurrentUser().userId
    try {
      userInvitationAuthorizationHelper.authorizeInvitationAdmin(id, currentUserId)
    } catch (e: Exception) {
      throw OperationNotAllowedException("Admin authorization failed for invitation id: $id and user id: $currentUserId", e)
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
