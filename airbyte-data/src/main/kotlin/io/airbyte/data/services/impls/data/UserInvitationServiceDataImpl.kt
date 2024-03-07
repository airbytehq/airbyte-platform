package io.airbyte.data.services.impls.data

import io.airbyte.config.ConfigSchema
import io.airbyte.config.InvitationStatus
import io.airbyte.config.Permission
import io.airbyte.config.ScopeType
import io.airbyte.config.UserInvitation
import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.data.repositories.PermissionRepository
import io.airbyte.data.repositories.UserInvitationRepository
import io.airbyte.data.services.UserInvitationService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.micronaut.transaction.annotation.Transactional
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
open class UserInvitationServiceDataImpl(
  private val userInvitationRepository: UserInvitationRepository,
  private val permissionRepository: PermissionRepository,
) : UserInvitationService {
  override fun getUserInvitationByInviteCode(inviteCode: String): UserInvitation {
    return userInvitationRepository.findByInviteCode(inviteCode).orElseThrow {
      ConfigNotFoundException(ConfigSchema.USER_INVITATION, inviteCode)
    }.toConfigModel()
  }

  override fun createUserInvitation(invitation: UserInvitation): UserInvitation {
    return userInvitationRepository.save(invitation.toEntity()).toConfigModel()
  }

  @Transactional("config")
  override fun acceptUserInvitation(
    inviteCode: String,
    invitedUserId: UUID,
  ): UserInvitation {
    // fetch the invitation by code
    val invitation =
      userInvitationRepository.findByInviteCode(inviteCode).orElseThrow {
        ConfigNotFoundException(ConfigSchema.USER_INVITATION, inviteCode)
      }.toConfigModel()

    if (invitation.status != InvitationStatus.PENDING) {
      throw IllegalStateException("Invitation status is not pending: ${invitation.status}")
    }

    // create a new permission record according to the invitation
    val permission =
      Permission().apply {
        userId = invitedUserId
        permissionType = invitation.permissionType
        when (invitation.scopeType) {
          ScopeType.ORGANIZATION -> organizationId = invitation.scopeId
          ScopeType.WORKSPACE -> workspaceId = invitation.scopeId
          else -> throw IllegalStateException("Unknown scope type: ${invitation.scopeType}")
        }
      }
    permissionRepository.save(permission.toEntity())

    // update the invitation status to accepted
    invitation.status = InvitationStatus.ACCEPTED
    val updatedInvitation = userInvitationRepository.update(invitation.toEntity())

    return updatedInvitation.toConfigModel()
  }

  override fun declineUserInvitation(
    inviteCode: String,
    invitedUserId: UUID,
  ): UserInvitation {
    TODO("Not yet implemented")
  }

  override fun cancelUserInvitation(inviteCode: String): UserInvitation {
    TODO("Not yet implemented")
  }
}
