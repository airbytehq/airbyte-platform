/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_ADMIN;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_ADMIN;

import io.airbyte.api.generated.UserInvitationApi;
import io.airbyte.api.model.generated.InviteCodeRequestBody;
import io.airbyte.api.model.generated.UserInvitationCreateRequestBody;
import io.airbyte.api.model.generated.UserInvitationRead;
import io.airbyte.commons.server.support.CurrentUserService;
import io.airbyte.config.User;
import io.airbyte.server.handlers.UserInvitationHandler;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Controller("/api/v1/user_invitations")
@Secured(SecurityRule.IS_AUTHENTICATED)
public class UserInvitationApiController implements UserInvitationApi {

  private final UserInvitationHandler userInvitationHandler;
  private final CurrentUserService currentUserService;

  public UserInvitationApiController(final UserInvitationHandler userInvitationHandler, final CurrentUserService currentUserService) {
    this.currentUserService = currentUserService;
    this.userInvitationHandler = userInvitationHandler;
  }

  @Get
  @Path("/{inviteCode}")
  @Override
  public UserInvitationRead getUserInvitation(@PathParam("inviteCode") final String inviteCode) {
    return ApiHelper.execute(() -> {
      final User currentUser = currentUserService.getCurrentUser();

      // note: this endpoint is accessible to all authenticated users, but the handler method throws an
      // exception if a user other than the invitee tries to get the invitation.
      return userInvitationHandler.getByInviteCode(inviteCode, currentUser);
    });
  }

  @Override
  @Secured({WORKSPACE_ADMIN, ORGANIZATION_ADMIN})
  public UserInvitationRead createUserInvitation(@Body final UserInvitationCreateRequestBody invitationCreateRequestBody) {
    return ApiHelper.execute(() -> {
      final User currentUser = currentUserService.getCurrentUser();

      return userInvitationHandler.create(invitationCreateRequestBody, currentUser.getUserId());
    });
  }

  @Override
  public UserInvitationRead acceptUserInvitation(@Body final InviteCodeRequestBody inviteCodeRequestBody) {
    return ApiHelper.execute(() -> {
      final User currentUser = currentUserService.getCurrentUser();

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
    // TODO only invite creator cancel the invitation
    throw new RuntimeException("Not yet implemented");
  }

}
