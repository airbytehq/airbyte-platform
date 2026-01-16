/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.ScopeType
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
   * Get a list of pending invitations for a given scope type and scope id.
   */
  fun getPendingInvitations(
    scopeType: ScopeType,
    scopeId: UUID,
  ): List<UserInvitation>

  /**
   * Create a new user invitation.
   */
  fun createUserInvitation(invitation: UserInvitation): UserInvitation

  /**
   * Accept a user invitation and create resulting permission record.
   */
  fun acceptUserInvitation(
    inviteCode: String,
    acceptingUserId: UUID,
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

  fun updateUserInvitations(invitations: List<UserInvitation>)
}

/**
 * Exception thrown when an operation on an invitation cannot be performed because it has an
 * unexpected status. For instance, trying to accept an invitation that is not pending.
 */
class InvitationStatusUnexpectedException(
  message: String,
) : Exception(message)

/**
 * Exception thrown when trying to create a duplicate invitation, ie creating new invitation with
 * the same email and scope as an existing pending invitation.
 */
class InvitationDuplicateException(
  message: String,
) : Exception(message)

/**
 * Exception thrown when trying to create an invitation for a scope where the user already has permission for that scope.
 */
class InvitationPermissionOverlapException(
  message: String,
) : Exception(message)
