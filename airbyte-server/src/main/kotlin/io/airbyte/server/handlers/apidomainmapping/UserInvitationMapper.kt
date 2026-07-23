/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers.apidomainmapping

import io.airbyte.api.model.generated.PermissionType
import io.airbyte.api.model.generated.ScopeType
import io.airbyte.api.model.generated.UserInvitationCreateRequestBody
import io.airbyte.api.model.generated.UserInvitationRead
import io.airbyte.api.model.generated.UserInvitationStatus
import io.airbyte.config.InvitationStatus
import io.airbyte.config.Permission
import io.airbyte.config.UserInvitation
import jakarta.inject.Singleton
import java.util.Locale

@Singleton
class UserInvitationMapper {
  // API Create request to Domain
  fun toDomain(apiCreateRequest: UserInvitationCreateRequestBody): UserInvitation {
    // This only converts fields from the creation request to the domain object, it does not
    // populate values for the other fields, like status, inviteCode, etc.
    return UserInvitation()
      .withInvitedEmail(apiCreateRequest.invitedEmail)
      .withScopeId(apiCreateRequest.scopeId)
      .withScopeType(toDomain(apiCreateRequest.scopeType))
      .withPermissionType(toDomain(apiCreateRequest.permissionType))
  }

  fun toDomain(api: ScopeType): io.airbyte.config.ScopeType =
    io.airbyte.config.ScopeType
      .fromValue(api.name.lowercase(Locale.getDefault()))

  fun toDomain(api: PermissionType): Permission.PermissionType = Permission.PermissionType.fromValue(api.name.lowercase(Locale.getDefault()))

  fun toDomain(api: UserInvitationStatus): InvitationStatus = InvitationStatus.fromValue(api.name.lowercase(Locale.getDefault()))

  // Domain to API
  fun toApi(domain: UserInvitation): UserInvitationRead =
    UserInvitationRead()
      .id(domain.id)
      .inviteCode(domain.inviteCode)
      .inviterUserId(domain.inviterUserId)
      .invitedEmail(domain.invitedEmail)
      .scopeId(domain.scopeId)
      .scopeType(toApi(domain.scopeType))
      .permissionType(toApi(domain.permissionType))
      .status(toApi(domain.status))
      .createdAt(domain.createdAt)
      .updatedAt(domain.updatedAt)

  fun toApi(domain: io.airbyte.config.ScopeType): ScopeType = ScopeType.fromValue(domain.name.lowercase(Locale.getDefault()))

  fun toApi(domain: Permission.PermissionType): PermissionType = PermissionType.fromValue(domain.name.lowercase(Locale.getDefault()))

  fun toApi(domain: InvitationStatus): UserInvitationStatus = UserInvitationStatus.fromValue(domain.name.lowercase(Locale.getDefault()))
}
