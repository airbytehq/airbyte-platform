/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Permission
import io.airbyte.config.ScopeType
import io.airbyte.config.UserInvitation
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.UserInvitationService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.AuthorizationTarget
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

data class SavedAuthorizationPermission(
  val target: AuthorizationTarget,
  val userId: Optional<UUID>,
  val groupId: Optional<UUID>,
  val type: Permission.PermissionType,
)

data class SavedAuthorizationInvitation(
  val target: AuthorizationTarget,
  val email: String,
  val permissionType: Permission.PermissionType,
)

data class AuthorizationUserIdentity(
  val userId: UUID,
  val email: Optional<String>,
)

/** Immutable metadata only. Group mutation, SCIM lifecycle, invitation delivery and permission writes stay in business services. */
@Singleton
class MembershipAuthorizationDataService(
  private val data: AuthorizationDataService,
  private val permissions: PermissionService,
  private val users: UserPersistence,
  private val invitations: UserInvitationService,
) {
  /** Group permission deletion consumes this saved parent, including permissions for deleted workspaces. */
  fun permissionGroup(permissionId: UUID): Optional<UUID> {
    val permission = resource { permissions.getPermission(permissionId) }
    if (permission.permissionId != permissionId) throw AuthorizationResourceNotFoundException()
    return Optional.ofNullable(permission.groupId)
  }

  fun permission(permissionId: UUID): SavedAuthorizationPermission {
    val permission = resource { permissions.getPermission(permissionId) }
    if (permission.permissionId != permissionId) throw AuthorizationResourceNotFoundException()
    val workspace = permission.workspaceId
    val organization = permission.organizationId
    if (workspace != null && organization != null) throw AuthorizationResourceNotFoundException()
    val type = permission.permissionType ?: throw AuthorizationResourceNotFoundException()
    val target =
      when {
        workspace != null -> AuthorizationTarget.Workspace(data.workspaceAndOrganization(workspace))
        organization != null -> AuthorizationTarget.Organization(data.organization(organization))
        type == Permission.PermissionType.INSTANCE_ADMIN || type == Permission.PermissionType.DATAPLANE -> AuthorizationTarget.Global
        else -> throw AuthorizationResourceNotFoundException()
      }
    return SavedAuthorizationPermission(target, Optional.ofNullable(permission.userId), Optional.ofNullable(permission.groupId), type)
  }

  fun isDirectOrganizationMember(
    userId: UUID,
    organizationId: UUID,
  ): Boolean {
    data.requireUser(userId)
    return permissions.getPermissionsForUser(userId).any { it.userId == userId && it.organizationId == organizationId }
  }

  fun invitationById(id: UUID): SavedAuthorizationInvitation {
    val invitation = resource { invitations.getUserInvitationById(id) }
    if (invitation.id != id) throw AuthorizationResourceNotFoundException()
    return invitation(invitation)
  }

  fun invitationByCode(code: String): SavedAuthorizationInvitation {
    val invitation = resource { invitations.getUserInvitationByInviteCode(code) }
    if (invitation.inviteCode != code) throw AuthorizationResourceNotFoundException()
    return invitation(invitation)
  }

  fun currentIdentity(subject: String): AuthorizationUserIdentity {
    val user = users.getUserByAuthId(subject).orElseThrow()
    check(user.authUserId == subject) { "Saved user does not match the authenticated subject" }
    return identity(user)
  }

  fun defaultIdentity(): AuthorizationUserIdentity = identity(users.getDefaultUser().orElseThrow())

  private fun identity(user: AuthenticatedUser): AuthorizationUserIdentity =
    AuthorizationUserIdentity(
      user.userId ?: throw AuthorizationResourceNotFoundException(),
      Optional.ofNullable(user.email),
    )

  private fun invitation(invitation: UserInvitation): SavedAuthorizationInvitation {
    val scope = invitation.scopeId ?: throw AuthorizationResourceNotFoundException()
    val target =
      when (invitation.scopeType) {
        ScopeType.WORKSPACE -> AuthorizationTarget.Workspace(data.workspaceAndOrganization(scope))
        ScopeType.ORGANIZATION -> AuthorizationTarget.Organization(data.organization(scope))
        null -> throw AuthorizationResourceNotFoundException()
      }
    return SavedAuthorizationInvitation(
      target,
      invitation.invitedEmail ?: throw AuthorizationResourceNotFoundException(),
      invitation.permissionType ?: throw AuthorizationResourceNotFoundException(),
    )
  }

  private fun <T> resource(load: () -> T): T =
    try {
      load()
    } catch (_: io.airbyte.data.ConfigNotFoundException) {
      throw AuthorizationResourceNotFoundException()
    } catch (_: io.airbyte.config.persistence.ConfigNotFoundException) {
      throw AuthorizationResourceNotFoundException()
    }
}
