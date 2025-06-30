/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.ScopeType
import io.airbyte.config.UserInvitation
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.repositories.PermissionRepository
import io.airbyte.data.repositories.UserInvitationRepository
import io.airbyte.data.repositories.entities.Permission
import io.airbyte.data.services.InvitationDuplicateException
import io.airbyte.data.services.InvitationPermissionOverlapException
import io.airbyte.data.services.InvitationStatusUnexpectedException
import io.airbyte.data.services.UserInvitationService
import io.airbyte.data.services.impls.data.mappers.EntityInvitationStatus
import io.airbyte.data.services.impls.data.mappers.EntityScopeType
import io.airbyte.data.services.impls.data.mappers.EntityUserInvitation
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.micronaut.transaction.annotation.Transactional
import jakarta.inject.Singleton
import java.time.OffsetDateTime
import java.util.UUID

@Singleton
open class UserInvitationServiceDataImpl(
  private val userInvitationRepository: UserInvitationRepository,
  private val permissionRepository: PermissionRepository,
) : UserInvitationService {
  override fun getUserInvitationByInviteCode(inviteCode: String): UserInvitation =
    userInvitationRepository
      .findByInviteCode(inviteCode)
      .orElseThrow {
        ConfigNotFoundException(ConfigNotFoundType.USER_INVITATION, inviteCode)
      }.toConfigModel()

  override fun getPendingInvitations(
    scopeType: ScopeType,
    scopeId: UUID,
  ): List<UserInvitation> =
    userInvitationRepository
      .findByStatusAndScopeTypeAndScopeId(
        EntityInvitationStatus.pending,
        scopeType.toEntity(),
        scopeId,
      ).map { it.toConfigModel() }

  override fun createUserInvitation(invitation: UserInvitation): UserInvitation {
    // throw an exception if a pending invitation already exists for the same email and scope
    val existingInvitations =
      userInvitationRepository.findByStatusAndScopeTypeAndScopeIdAndInvitedEmail(
        EntityInvitationStatus.pending,
        invitation.scopeType.toEntity(),
        invitation.scopeId,
        invitation.invitedEmail,
      )

    throwIfUserHasPermissionInScope(invitation.invitedEmail, invitation.scopeId)

    if (existingInvitations.isNotEmpty()) {
      throw InvitationDuplicateException(
        "A pending invitation already exists for InvitedEmail: ${invitation.invitedEmail}, ScopeType: ${invitation.scopeType} " +
          "and ScopeId: ${invitation.scopeId}",
      )
    }

    return userInvitationRepository.save(invitation.toEntity()).toConfigModel()
  }

  @Transactional("config")
  override fun acceptUserInvitation(
    inviteCode: String,
    acceptingUserId: UUID,
  ): UserInvitation {
    // fetch the invitation by code
    val invitation =
      userInvitationRepository.findByInviteCode(inviteCode).orElseThrow {
        ConfigNotFoundException(ConfigNotFoundType.USER_INVITATION, inviteCode)
      }

    // mark the invitation status as expired if expiresAt is in the past
    if (invitation.expiresAt.isBefore(OffsetDateTime.now())) {
      invitation.status = EntityInvitationStatus.expired
      userInvitationRepository.update(invitation)
    }

    // throw an exception if the invitation is not pending. Note that this will also
    // catch the case where the invitation is expired.
    throwIfNotPending(invitation)

    // create a new permission record according to the invitation
    Permission(
      id = UUID.randomUUID(),
      userId = acceptingUserId,
      permissionType = invitation.permissionType,
    ).apply {
      when (invitation.scopeType) {
        EntityScopeType.organization -> organizationId = invitation.scopeId
        EntityScopeType.workspace -> workspaceId = invitation.scopeId
      }
    }.let { permissionRepository.save(it) }

    // mark the invitation as accepted
    invitation.status = EntityInvitationStatus.accepted
    invitation.acceptedByUserId = acceptingUserId
    val updatedInvitation = userInvitationRepository.update(invitation)

    return updatedInvitation.toConfigModel()
  }

  override fun declineUserInvitation(
    inviteCode: String,
    invitedUserId: UUID,
  ): UserInvitation {
    TODO("Not yet implemented")
  }

  override fun cancelUserInvitation(inviteCode: String): UserInvitation {
    val invitation =
      userInvitationRepository.findByInviteCode(inviteCode).orElseThrow {
        ConfigNotFoundException(ConfigNotFoundType.USER_INVITATION, inviteCode)
      }

    throwIfNotPending(invitation)

    invitation.status = EntityInvitationStatus.cancelled
    val updatedInvitation = userInvitationRepository.update(invitation)

    return updatedInvitation.toConfigModel()
  }

  private fun throwIfNotPending(invitation: EntityUserInvitation) {
    if (invitation.status != EntityInvitationStatus.pending) {
      throw InvitationStatusUnexpectedException(
        "Expected invitation for ScopeType: ${invitation.scopeType} and ScopeId: ${invitation.scopeId} to " +
          "be PENDING, but instead it had Status: ${invitation.status}",
      )
    }
  }

  /**
   * Throws an exception if the invited user already has permissions in the scope.
   * Explicitly does not check redundant permissions for invites bc the frontend calls through to the createPermissions API which does that.
   */
  private fun throwIfUserHasPermissionInScope(
    invitedEmail: String,
    scopeId: UUID,
  ) {
    val existingUserPermissions = permissionRepository.findByUserEmail(invitedEmail)
    val scopedPermissions =
      existingUserPermissions.filter {
        it.organizationId == scopeId || it.workspaceId == scopeId
      }
    if (scopedPermissions.isNotEmpty()) {
      throw InvitationPermissionOverlapException("User with email $invitedEmail already has permissions in scope $scopeId")
    }
  }
}
