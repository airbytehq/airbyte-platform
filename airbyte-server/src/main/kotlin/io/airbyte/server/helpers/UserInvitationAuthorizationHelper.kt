package io.airbyte.server.helpers

import io.airbyte.api.model.generated.PermissionCheckRead.StatusEnum
import io.airbyte.api.model.generated.PermissionCheckRequest
import io.airbyte.api.model.generated.PermissionType
import io.airbyte.commons.server.errors.OperationNotAllowedException
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.config.ScopeType
import io.airbyte.data.services.UserInvitationService
import jakarta.inject.Singleton
import java.util.UUID

/**
 * Helper class for performing authorization checks related to User Invitations.
 */
@Singleton
class UserInvitationAuthorizationHelper(
  private val userInvitationService: UserInvitationService,
  private val permissionHandler: PermissionHandler,
) {
  /**
   * Authorizes a user as an admin for a given invitation. Based on the scope of the invitation,
   * checks if the user has necessary workspace/organization admin permissions.
   *
   * @throws OperationNotAllowedException if authorization fails.
   */
  @Throws(OperationNotAllowedException::class)
  fun authorizeInvitationAdmin(
    inviteCode: String,
    userId: UUID,
  ) {
    try {
      val invitation = userInvitationService.getUserInvitationByInviteCode(inviteCode)
      when (invitation.scopeType) {
        ScopeType.WORKSPACE -> authorizeWorkspaceInvitationAdmin(invitation.scopeId, userId)
        ScopeType.ORGANIZATION -> authorizeOrganizationInvitationAdmin(invitation.scopeId, userId)
        null -> throw OperationNotAllowedException("Invitation $inviteCode has no scope type")
      }
    } catch (e: Exception) {
      // always explicitly throw a 403 if anything goes wrong during authorization
      throw OperationNotAllowedException("Could not authorize $userId for invitation $inviteCode", e)
    }
  }

  private fun authorizeWorkspaceInvitationAdmin(
    workspaceId: UUID,
    userId: UUID,
  ) {
    val result =
      permissionHandler.checkPermissions(
        PermissionCheckRequest()
          .userId(userId)
          .permissionType(PermissionType.WORKSPACE_ADMIN)
          .workspaceId(workspaceId),
      )

    if (!result.status.equals(StatusEnum.SUCCEEDED)) {
      throw OperationNotAllowedException("User $userId is not an admin of workspace $workspaceId")
    }
  }

  private fun authorizeOrganizationInvitationAdmin(
    organizationId: UUID,
    userId: UUID,
  ) {
    val result =
      permissionHandler.checkPermissions(
        PermissionCheckRequest()
          .userId(userId)
          .permissionType(PermissionType.ORGANIZATION_ADMIN)
          .organizationId(organizationId),
      )

    if (!result.status.equals(StatusEnum.SUCCEEDED)) {
      throw OperationNotAllowedException("User $userId is not an admin of organization $organizationId")
    }
  }
}
