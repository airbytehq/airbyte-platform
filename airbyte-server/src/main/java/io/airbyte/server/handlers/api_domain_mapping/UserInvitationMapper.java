/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers.api_domain_mapping;

import io.airbyte.api.model.generated.UserInvitationCreateRequestBody;
import io.airbyte.api.model.generated.UserInvitationRead;
import io.airbyte.api.model.generated.UserInvitationStatus;
import io.airbyte.config.InvitationStatus;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.ScopeType;
import io.airbyte.config.UserInvitation;
import jakarta.inject.Singleton;

@Singleton
public class UserInvitationMapper {

  // API Create request to Domain
  public UserInvitation toDomain(final UserInvitationCreateRequestBody apiCreateRequest) {
    // This only converts fields from the creation request to the domain object, it does not
    // populate values for the other fields, like status, inviteCode, etc.
    return new UserInvitation()
        .withInvitedEmail(apiCreateRequest.getInvitedEmail())
        .withScopeId(apiCreateRequest.getScopeId())
        .withScopeType(toDomain(apiCreateRequest.getScopeType()))
        .withPermissionType(toDomain(apiCreateRequest.getPermissionType()));
  }

  public ScopeType toDomain(final io.airbyte.api.model.generated.ScopeType api) {
    return ScopeType.fromValue(api.name().toLowerCase());
  }

  public PermissionType toDomain(final io.airbyte.api.model.generated.PermissionType api) {
    return PermissionType.fromValue(api.name().toLowerCase());
  }

  public InvitationStatus toDomain(final UserInvitationStatus api) {
    return InvitationStatus.fromValue(api.name().toLowerCase());
  }

  // Domain to API
  public UserInvitationRead toApi(final UserInvitation domain) {
    return new UserInvitationRead()
        .id(domain.getId())
        .inviteCode(domain.getInviteCode())
        .inviterUserId(domain.getInviterUserId())
        .invitedEmail(domain.getInvitedEmail())
        .scopeId(domain.getScopeId())
        .scopeType(toApi(domain.getScopeType()))
        .permissionType(toApi(domain.getPermissionType()))
        .status(toApi(domain.getStatus()))
        .createdAt(domain.getCreatedAt())
        .updatedAt(domain.getUpdatedAt());
  }

  public io.airbyte.api.model.generated.ScopeType toApi(final ScopeType domain) {
    return io.airbyte.api.model.generated.ScopeType.fromValue(domain.name().toLowerCase());
  }

  public io.airbyte.api.model.generated.PermissionType toApi(final PermissionType domain) {
    return io.airbyte.api.model.generated.PermissionType.fromValue(domain.name().toLowerCase());
  }

  public UserInvitationStatus toApi(final InvitationStatus domain) {
    return UserInvitationStatus.fromValue(domain.name().toLowerCase());
  }

}
