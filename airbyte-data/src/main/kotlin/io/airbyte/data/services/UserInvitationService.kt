package io.airbyte.data.services

import io.airbyte.config.UserInvitation
import java.util.UUID

/**
 * A service that manages user invitations.
 */
interface UserInvitationService {
  /**
   * Get a user invitation by its unique invite code.
   */
  fun getUserInvitationByInviteCode(inviteCode: String): UserInvitation

  /**
   * Create a new user invitation.
   */
  fun createUserInvitation(invitation: UserInvitation): UserInvitation

  /**
   * Accept a user invitation and create resulting permission record.
   */
  fun acceptUserInvitation(
    inviteCode: String,
    invitedUserId: UUID,
  ): UserInvitation

  /**
   * Decline a user invitation.
   */
  fun declineUserInvitation(
    inviteCode: String,
    invitedUserId: UUID,
  ): UserInvitation

  /**
   * Cancel a user invitation.
   */
  fun cancelUserInvitation(inviteCode: String): UserInvitation
}
