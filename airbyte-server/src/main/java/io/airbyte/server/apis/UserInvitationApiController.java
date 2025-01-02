/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_ADMIN;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_READER;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_ADMIN;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_READER;

import io.airbyte.api.generated.UserInvitationApi;
import io.airbyte.api.model.generated.InviteCodeRequestBody;
import io.airbyte.api.model.generated.UserInvitationCreateRequestBody;
import io.airbyte.api.model.generated.UserInvitationCreateResponse;
import io.airbyte.api.model.generated.UserInvitationListRequestBody;
import io.airbyte.api.model.generated.UserInvitationRead;
import io.airbyte.commons.server.errors.OperationNotAllowedException;
import io.airbyte.commons.server.support.CurrentUserService;
import io.airbyte.config.AuthenticatedUser;
import io.airbyte.server.handlers.UserInvitationHandler;
import io.airbyte.server.helpers.UserInvitationAuthorizationHelper;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import java.util.List;
import java.util.UUID;

@Controller("/api/v1/user_invitations")
@Secured(SecurityRule.IS_AUTHENTICATED)
public class UserInvitationApiController implements UserInvitationApi {

  private final UserInvitationHandler userInvitationHandler;
  private final CurrentUserService currentUserService;
  private final UserInvitationAuthorizationHelper userInvitationAuthorizationHelper;

  public UserInvitationApiController(final UserInvitationHandler userInvitationHandler,
                                     final CurrentUserService currentUserService,
                                     final UserInvitationAuthorizationHelper userInvitationAuthorizationHelper) {
    this.currentUserService = currentUserService;
    this.userInvitationHandler = userInvitationHandler;
    this.userInvitationAuthorizationHelper = userInvitationAuthorizationHelper;
  }

  @Get
  @Path("/by_code/{inviteCode}")
  @Override
  public UserInvitationRead getUserInvitation(@PathParam("inviteCode") final String inviteCode) {
    return ApiHelper.execute(() -> {
      final AuthenticatedUser currentUser = currentUserService.getCurrentUser();

      // note: this endpoint is accessible to all authenticated users, but the handler method throws an
      // exception if a user other than the invitee tries to get the invitation.
      return userInvitationHandler.getByInviteCode(inviteCode, currentUser);
    });
  }

  @Post
  @Path("/list_pending")
  @Override
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  public List<UserInvitationRead> listPendingInvitations(@Body final UserInvitationListRequestBody invitationListRequestBody) {
    return userInvitationHandler.getPendingInvitations(invitationListRequestBody);
  }

  @Override
  @Secured({WORKSPACE_ADMIN, ORGANIZATION_ADMIN})
  public UserInvitationCreateResponse createUserInvitation(@Body final UserInvitationCreateRequestBody invitationCreateRequestBody) {
    return ApiHelper.execute(() -> {
      final AuthenticatedUser currentUser = currentUserService.getCurrentUser();

      return userInvitationHandler.createInvitationOrPermission(invitationCreateRequestBody, currentUser);
    });
  }

  @Override
  public UserInvitationRead acceptUserInvitation(@Body final InviteCodeRequestBody inviteCodeRequestBody) {
    return ApiHelper.execute(() -> {
      final AuthenticatedUser currentUser = currentUserService.getCurrentUser();

      // note: this endpoint is accessible to all authenticated users, but the handler method throws an
      // exception if a user other than the invitee tries to accept the invitation.
      return userInvitationHandler.accept(inviteCodeRequestBody, currentUser);
    });
  }

  @Override
  public UserInvitationRead declineUserInvitation(@Body final InviteCodeRequestBody inviteCodeRequestBody) {
    return ApiHelper.execute(() -> {
      // TODO only the invitee can decline the invitation
      throw new RuntimeException("Not yet implemented");
    });
  }

  @Override
  public UserInvitationRead cancelUserInvitation(@Body final InviteCodeRequestBody inviteCodeRequestBody) {
    // note: this endpoint is accessible to all authenticated users, but `authorizeInvitationAdmin`
    // throws a 403 if a non-admin user of the invitation's scope tries to cancel it.
    return ApiHelper.execute(() -> {
      authorizeInvitationAdmin(inviteCodeRequestBody.getInviteCode());
      return userInvitationHandler.cancel(inviteCodeRequestBody);
    });
  }

  private void authorizeInvitationAdmin(final String inviteCode) {
    final UUID currentUserId = currentUserService.getCurrentUser().getUserId();
    try {
      userInvitationAuthorizationHelper.authorizeInvitationAdmin(inviteCode, currentUserId);
    } catch (final Exception e) {
      throw new OperationNotAllowedException("Admin authorization failed for invite code: " + inviteCode + " and user id: " + currentUserId, e);
    }
  }

}
