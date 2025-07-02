/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.ScopeType
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.repositories.PermissionRepository
import io.airbyte.data.repositories.UserInvitationRepository
import io.airbyte.data.repositories.entities.Permission
import io.airbyte.data.repositories.entities.UserInvitation
import io.airbyte.data.services.InvitationDuplicateException
import io.airbyte.data.services.InvitationPermissionOverlapException
import io.airbyte.data.services.InvitationStatusUnexpectedException
import io.airbyte.data.services.impls.data.mappers.EntityInvitationStatus
import io.airbyte.data.services.impls.data.mappers.EntityPermissionType
import io.airbyte.data.services.impls.data.mappers.EntityScopeType
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.mockk.clearAllMocks
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.Optional
import java.util.UUID

internal class UserInvitationServiceDataImplTest {
  private val userInvitationRepository = mockk<UserInvitationRepository>()
  private val permissionRepository = mockk<PermissionRepository>()
  private val userInvitationService = UserInvitationServiceDataImpl(userInvitationRepository, permissionRepository)

  private val invitation =
    UserInvitation(
      id = UUID.randomUUID(),
      inviteCode = "some-code",
      inviterUserId = UUID.randomUUID(),
      invitedEmail = "invited@airbyte.io",
      scopeId = UUID.randomUUID(),
      scopeType = EntityScopeType.workspace,
      permissionType = EntityPermissionType.workspace_admin,
      status = EntityInvitationStatus.pending,
      createdAt = OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(java.time.temporal.ChronoUnit.SECONDS),
      updatedAt = OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(java.time.temporal.ChronoUnit.SECONDS),
      expiresAt = OffsetDateTime.now(ZoneOffset.UTC).plusDays(7).truncatedTo(java.time.temporal.ChronoUnit.SECONDS),
    )

  @BeforeEach
  fun reset() {
    clearAllMocks()
  }

  @Test
  fun `test get user invitation by invite code`() {
    every { userInvitationRepository.findByInviteCode(invitation.inviteCode) } returns Optional.of(invitation)

    val invitation = userInvitationService.getUserInvitationByInviteCode(invitation.inviteCode)
    assert(invitation == this.invitation.toConfigModel())

    verify { userInvitationRepository.findByInviteCode(this@UserInvitationServiceDataImplTest.invitation.inviteCode) }
  }

  @Test
  fun `test get user invitation by non-existent code throws`() {
    every { userInvitationRepository.findByInviteCode(any()) } returns Optional.empty()

    assertThrows<ConfigNotFoundException> { userInvitationService.getUserInvitationByInviteCode("non-existent-code") }

    verify { userInvitationRepository.findByInviteCode("non-existent-code") }
  }

  @Test
  fun `test create user invitation`() {
    every { userInvitationRepository.findByStatusAndScopeTypeAndScopeIdAndInvitedEmail(any(), any(), any(), any()) } returns emptyList()
    every { permissionRepository.findByUserEmail(any()) } returns emptyList()
    every { userInvitationRepository.save(invitation) } returns invitation

    val result = userInvitationService.createUserInvitation(invitation.toConfigModel())
    assert(result == invitation.toConfigModel())

    verify { userInvitationRepository.save(invitation) }
  }

  @Test
  fun `test create duplicate user invitation throws`() {
    every { userInvitationRepository.findByStatusAndScopeTypeAndScopeIdAndInvitedEmail(any(), any(), any(), any()) } returns listOf(invitation)

    every { permissionRepository.findByUserEmail(any()) } returns emptyList()
    assertThrows<InvitationDuplicateException> { userInvitationService.createUserInvitation(invitation.toConfigModel()) }

    verify(exactly = 0) { userInvitationRepository.save(invitation) }
  }

  @Test
  fun `test create existing permission user invitation throws`() {
    every { userInvitationRepository.findByStatusAndScopeTypeAndScopeIdAndInvitedEmail(any(), any(), any(), any()) } returns emptyList()

    val permission =
      Permission(
        id = UUID.randomUUID(),
        userId = UUID.randomUUID(),
        workspaceId = invitation.scopeId,
        organizationId = null,
        permissionType = invitation.permissionType,
      )

    every { permissionRepository.findByUserEmail(any()) } returns listOf(permission)
    assertThrows<InvitationPermissionOverlapException> { userInvitationService.createUserInvitation(invitation.toConfigModel()) }

    verify(exactly = 0) { userInvitationRepository.save(invitation) }
  }

  @Test
  fun `test accept user invitation`() {
    val invitedUserId = UUID.randomUUID()
    val expectedUpdatedInvitation =
      invitation.copy(
        status = EntityInvitationStatus.accepted,
        acceptedByUserId = invitedUserId,
      )

    every { userInvitationRepository.findByInviteCode(invitation.inviteCode) } returns Optional.of(invitation)
    every { userInvitationRepository.update(expectedUpdatedInvitation) } returns expectedUpdatedInvitation
    every { permissionRepository.save(any()) } returns mockk()

    userInvitationService.acceptUserInvitation(invitation.inviteCode, invitedUserId)

    // verify the expected permission is created for the invited user
    verify {
      permissionRepository.save(
        match<Permission> {
          it.userId == invitedUserId &&
            it.permissionType == invitation.permissionType &&
            it.workspaceId == invitation.scopeId &&
            it.organizationId == null
        },
      )
    }

    // verify the invitation status is updated to accepted
    verify { userInvitationRepository.update(expectedUpdatedInvitation) }
  }

  @ParameterizedTest
  @EnumSource(value = EntityInvitationStatus::class)
  fun `test accept user invitation fails if not pending`(status: EntityInvitationStatus) {
    if (status == EntityInvitationStatus.pending) {
      return // not testing this case
    }

    val invitedUserId = UUID.randomUUID()
    val invitation = this.invitation.copy(status = status)

    every { userInvitationRepository.findByInviteCode(invitation.inviteCode) } returns Optional.of(invitation)

    assertThrows<InvitationStatusUnexpectedException> { userInvitationService.acceptUserInvitation(invitation.inviteCode, invitedUserId) }

    verify(exactly = 0) { userInvitationRepository.update(any()) }
  }

  @Test
  fun `test accept user invitation fails if expired`() {
    val invitedUserId = UUID.randomUUID()
    val expiredInvitation =
      invitation.copy(
        status = EntityInvitationStatus.pending,
        expiresAt = OffsetDateTime.now(ZoneOffset.UTC).minusDays(1),
      )
    val expectedUpdatedInvitation = expiredInvitation.copy(status = EntityInvitationStatus.expired)

    every { userInvitationRepository.findByInviteCode(expiredInvitation.inviteCode) } returns Optional.of(expiredInvitation)
    every { userInvitationRepository.update(expectedUpdatedInvitation) } returns expectedUpdatedInvitation

    assertThrows<InvitationStatusUnexpectedException> { userInvitationService.acceptUserInvitation(expiredInvitation.inviteCode, invitedUserId) }

    verify { userInvitationRepository.update(expectedUpdatedInvitation) }
  }

  @Test
  fun `test get pending invitations`() {
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val mockWorkspaceInvitations = listOf(invitation, invitation.copy(id = UUID.randomUUID()))
    val mockOrganizationInvitations = listOf(invitation.copy(id = UUID.randomUUID()), invitation.copy(id = UUID.randomUUID()))

    every {
      userInvitationRepository.findByStatusAndScopeTypeAndScopeId(EntityInvitationStatus.pending, EntityScopeType.workspace, workspaceId)
    } returns mockWorkspaceInvitations
    every {
      userInvitationRepository.findByStatusAndScopeTypeAndScopeId(EntityInvitationStatus.pending, EntityScopeType.organization, organizationId)
    } returns mockOrganizationInvitations

    val workspaceResult = userInvitationService.getPendingInvitations(ScopeType.WORKSPACE, workspaceId)
    val organizationResult = userInvitationService.getPendingInvitations(ScopeType.ORGANIZATION, organizationId)

    verify(exactly = 1) {
      userInvitationRepository.findByStatusAndScopeTypeAndScopeId(EntityInvitationStatus.pending, EntityScopeType.workspace, workspaceId)
    }
    verify(exactly = 1) {
      userInvitationRepository.findByStatusAndScopeTypeAndScopeId(EntityInvitationStatus.pending, EntityScopeType.organization, organizationId)
    }
    confirmVerified(userInvitationRepository)

    assert(workspaceResult == mockWorkspaceInvitations.map { it.toConfigModel() })
    assert(organizationResult == mockOrganizationInvitations.map { it.toConfigModel() })
  }

  @Test
  fun `test cancel invitation`() {
    val expectedUpdatedInvitation = invitation.copy(status = EntityInvitationStatus.cancelled)
    every { userInvitationRepository.findByInviteCode(invitation.inviteCode) } returns Optional.of(invitation)
    every { userInvitationRepository.update(expectedUpdatedInvitation) } returns expectedUpdatedInvitation

    userInvitationService.cancelUserInvitation(invitation.inviteCode)

    verify { userInvitationRepository.update(expectedUpdatedInvitation) }
  }
}
