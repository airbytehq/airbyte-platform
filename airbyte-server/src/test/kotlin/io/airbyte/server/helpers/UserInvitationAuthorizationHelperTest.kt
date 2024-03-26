package io.airbyte.server.helpers

import io.airbyte.api.model.generated.PermissionCheckRead
import io.airbyte.commons.server.errors.OperationNotAllowedException
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.config.ScopeType
import io.airbyte.config.UserInvitation
import io.airbyte.data.services.UserInvitationService
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.util.UUID

class UserInvitationAuthorizationHelperTest {
  private val userInvitationService = mockk<UserInvitationService>()
  private val permissionHandler = mockk<PermissionHandler>()

  private lateinit var authorizationHelper: UserInvitationAuthorizationHelper
  private val userId = UUID.randomUUID()
  private val inviteCode = "test-invite-code"
  private val invitation = UserInvitation().withInviteCode(inviteCode).withScopeId(UUID.randomUUID())

  @BeforeEach
  fun setUp() {
    authorizationHelper = UserInvitationAuthorizationHelper(userInvitationService, permissionHandler)
  }

  @ParameterizedTest
  @EnumSource(ScopeType::class)
  fun `successful permission check does not throw`(scopeType: ScopeType) {
    invitation.scopeType = scopeType
    every { userInvitationService.getUserInvitationByInviteCode(inviteCode) } returns invitation
    every { permissionHandler.checkPermissions(any()) } returns PermissionCheckRead().status(PermissionCheckRead.StatusEnum.SUCCEEDED)

    assertDoesNotThrow { authorizationHelper.authorizeInvitationAdmin(inviteCode, userId) }
  }

  @ParameterizedTest
  @EnumSource(ScopeType::class)
  fun `failed permission check throws`(scopeType: ScopeType) {
    invitation.scopeType = scopeType
    every { userInvitationService.getUserInvitationByInviteCode(inviteCode) } returns invitation
    every { permissionHandler.checkPermissions(any()) } returns PermissionCheckRead().status(PermissionCheckRead.StatusEnum.FAILED)

    assertThrows<OperationNotAllowedException> { authorizationHelper.authorizeInvitationAdmin(inviteCode, userId) }
  }

  @Test
  fun `authorizeInvitationAdmin should handle exceptions from UserInvitationService`() {
    every { userInvitationService.getUserInvitationByInviteCode(inviteCode) } throws RuntimeException("Service exception")

    assertThrows<OperationNotAllowedException> { authorizationHelper.authorizeInvitationAdmin(inviteCode, userId) }
  }
}
