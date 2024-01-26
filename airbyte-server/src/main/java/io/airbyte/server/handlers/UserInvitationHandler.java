/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers;

import io.airbyte.api.model.generated.InviteCodeRequestBody;
import io.airbyte.api.model.generated.UserInvitationCreateRequestBody;
import io.airbyte.api.model.generated.UserInvitationRead;
import io.airbyte.commons.server.errors.OperationNotAllowedException;
import io.airbyte.config.InvitationStatus;
import io.airbyte.config.User;
import io.airbyte.config.UserInvitation;
import io.airbyte.data.services.UserInvitationService;
import io.airbyte.server.handlers.api_domain_mapping.UserInvitationMapper;
import jakarta.inject.Singleton;
import java.util.UUID;

@Singleton
public class UserInvitationHandler {

  final UserInvitationService service;
  final UserInvitationMapper mapper;

  public UserInvitationHandler(final UserInvitationService service, final UserInvitationMapper mapper) {
    this.service = service;
    this.mapper = mapper;
  }

  public UserInvitationRead getByInviteCode(final String inviteCode, final User currentUser) {
    final UserInvitation invitation = service.getUserInvitationByInviteCode(inviteCode);

    if (!invitation.getInvitedEmail().equals(currentUser.getEmail())) {
      throw new OperationNotAllowedException("Invited email does not match current user email.");
    }

    return mapper.toApi(invitation);
  }

  public UserInvitationRead create(final UserInvitationCreateRequestBody req, final UUID currentUserId) {
    final UserInvitation model = mapper.toDomain(req);

    model.setInviterUserId(currentUserId);

    // For now, inviteCodes are simply UUIDs that are converted to strings, to virtually guarantee
    // uniqueness.
    // The column itself is a string, so if UUIDs prove to be cumbersome or too long, we can always
    // switch to
    // a different method of generating shorter, unique inviteCodes.
    model.setInviteCode(UUID.randomUUID().toString());

    // New UserInvitations are always created with a status of PENDING.
    model.setStatus(InvitationStatus.PENDING);

    final UserInvitation saved = service.createUserInvitation(model);
    return mapper.toApi(saved);
  }

  public UserInvitationRead accept(final InviteCodeRequestBody req, final User currentUser) {
    final UserInvitation invitation = service.getUserInvitationByInviteCode(req.getInviteCode());

    if (!invitation.getInvitedEmail().equals(currentUser.getEmail())) {
      throw new OperationNotAllowedException("Invited email does not match current user email.");
    }

    // TODO - ensure that only org-level invitation can be accepted by a user currently logged into that
    // org.
    // email is not enough, because a user can have multiple logins with the same associated email, ie
    // if they sign in through both SSO and via email/password.
    final UserInvitation accepted = service.acceptUserInvitation(req.getInviteCode(), currentUser.getUserId());

    return mapper.toApi(accepted);
  }

  // TODO implement `decline`

  // TODO implement `cancel`

}
