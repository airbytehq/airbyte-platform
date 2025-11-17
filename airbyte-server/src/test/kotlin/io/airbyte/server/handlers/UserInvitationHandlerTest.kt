/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import io.airbyte.analytics.TrackingClient
import io.airbyte.api.client.WebUrlHelper
import io.airbyte.api.model.generated.InviteCodeRequestBody
import io.airbyte.api.model.generated.PermissionType
import io.airbyte.api.model.generated.UserInvitationCreateRequestBody
import io.airbyte.api.model.generated.UserInvitationCreateResponse
import io.airbyte.api.model.generated.UserInvitationListRequestBody
import io.airbyte.api.model.generated.UserInvitationRead
import io.airbyte.commons.server.errors.ConflictException
import io.airbyte.commons.server.errors.OperationNotAllowedException
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.InvitationStatus
import io.airbyte.config.Permission
import io.airbyte.config.ScopeType
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.User
import io.airbyte.config.UserInvitation
import io.airbyte.config.UserPermission
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.services.InvitationDuplicateException
import io.airbyte.data.services.InvitationStatusUnexpectedException
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.UserInvitationService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.notification.CustomerIoEmailConfig
import io.airbyte.notification.CustomerIoEmailNotificationSender
import io.airbyte.server.handlers.apidomainmapping.UserInvitationMapper
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.Optional
import java.util.UUID

internal class UserInvitationHandlerTest {
  private lateinit var service: UserInvitationService
  private lateinit var mapper: UserInvitationMapper
  private lateinit var customerIoEmailNotificationSender: CustomerIoEmailNotificationSender
  private lateinit var webUrlHelper: WebUrlHelper
  private lateinit var workspaceService: WorkspaceService
  private lateinit var organizationService: OrganizationService
  private lateinit var userPersistence: UserPersistence
  private lateinit var permissionHandler: PermissionHandler
  private lateinit var trackingClient: TrackingClient
  private lateinit var handler: UserInvitationHandler

  @BeforeEach
  fun setup() {
    service = mockk()
    mapper = mockk()
    customerIoEmailNotificationSender = mockk()
    webUrlHelper = mockk()
    workspaceService = mockk()
    organizationService = mockk()
    userPersistence = mockk()
    permissionHandler = mockk()
    trackingClient = mockk()
    handler =
      UserInvitationHandler(
        service,
        mapper,
        customerIoEmailNotificationSender,
        webUrlHelper,
        workspaceService,
        organizationService,
        userPersistence,
        permissionHandler,
        trackingClient,
      )
  }

  @Nested
  inner class CreateInvitationOrPermission {
    private val currentUser: AuthenticatedUser =
      AuthenticatedUser().withUserId(UUID.randomUUID()).withEmail("current-user@airbyte.io").withName("Current User")
    private val webappBaseUrl = "https://test.airbyte.io"
    private val invitedEmail = "invited@airbyte.io"
    private val workspaceId: UUID = UUID.randomUUID()
    private val workspaceName = "workspace-name"
    private val orgId: UUID = UUID.randomUUID()
    private val userInvitationCreateRequestBody: UserInvitationCreateRequestBody =
      UserInvitationCreateRequestBody()
        .invitedEmail(invitedEmail)
        .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE)
        .scopeId(workspaceId)
        .permissionType(PermissionType.WORKSPACE_ADMIN)
    private val userInvitation: UserInvitation =
      UserInvitation()
        .withInvitedEmail(invitedEmail)
        .withScopeType(ScopeType.WORKSPACE)
        .withScopeId(workspaceId)
        .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)

    @Nested
    internal inner class CreateAndSendInvitation {
      private fun setupSendInvitationMocks() {
        every { webUrlHelper.baseUrl } returns webappBaseUrl
        every { service.createUserInvitation(userInvitation) } returns userInvitation
        every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false) } returns StandardWorkspace().withName(workspaceName)
        every { customerIoEmailNotificationSender.sendInviteToUser(any(), any(), any()) } returns Unit
      }

      @BeforeEach
      fun setup() {
        every { mapper.toDomain(userInvitationCreateRequestBody) } returns userInvitation
      }

      @Test
      fun testNewEmailWorkspaceInOrg() {
        setupSendInvitationMocks()

        // the workspace is in an org.
        every { workspaceService.getOrganizationIdFromWorkspaceId(workspaceId) } returns Optional.of(orgId)

        // no existing user has the invited email.
        every { userPersistence.getUserByEmail(invitedEmail) } returns Optional.empty()
        every { permissionHandler.listUsersInOrganization(orgId) } returns emptyList()

        // call the handler method under test.
        val result: UserInvitationCreateResponse =
          handler.createInvitationOrPermission(
            userInvitationCreateRequestBody,
            currentUser,
          )

        // make sure correct invite was created, email was sent, and result is correct.
        verifyInvitationCreatedAndEmailSentResult(result)
      }

      @Test
      fun testWorkspaceNotInAnyOrg() {
        setupSendInvitationMocks()

        // the workspace is not in any org.
        every { workspaceService.getOrganizationIdFromWorkspaceId(workspaceId) } returns Optional.empty()

        // call the handler method under test.
        val result: UserInvitationCreateResponse =
          handler.createInvitationOrPermission(
            userInvitationCreateRequestBody,
            currentUser,
          )

        // make sure correct invite was created, email was sent, and result is correct.
        verifyInvitationCreatedAndEmailSentResult(result)
      }

      @Test
      fun testExistingEmailButNotInWorkspaceOrg() {
        setupSendInvitationMocks()

        // the workspace is in an org.
        every { workspaceService.getOrganizationIdFromWorkspaceId(workspaceId) } returns Optional.of(orgId)

        // a user with the email exists, but is not in the workspace's org.
        val userWithEmail = User().withUserId(UUID.randomUUID()).withEmail(invitedEmail)
        every { userPersistence.getUserByEmail(invitedEmail) } returns Optional.of(userWithEmail)

        // the org has a user with a different email, but not the one we're inviting.
        val otherUserInOrg = User().withUserId(UUID.randomUUID()).withEmail("other@airbyte.io")
        every { permissionHandler.listUsersInOrganization(orgId) } returns listOf(UserPermission().withUser(otherUserInOrg))

        // call the handler method under test.
        val result: UserInvitationCreateResponse =
          handler.createInvitationOrPermission(
            userInvitationCreateRequestBody,
            currentUser,
          )

        // make sure correct invite was created, email was sent, and result is correct.
        verifyInvitationCreatedAndEmailSentResult(result)
      }

      @Test
      fun testThrowsConflictExceptionOnDuplicateInvitation() {
        every { workspaceService.getOrganizationIdFromWorkspaceId(workspaceId) } returns Optional.empty()
        every { service.createUserInvitation(userInvitation) } answers { throw InvitationDuplicateException("duplicate") }

        Assertions.assertThrows(
          ConflictException::class.java,
        ) {
          handler.createInvitationOrPermission(
            userInvitationCreateRequestBody,
            currentUser,
          )
        }
      }

      private fun verifyInvitationCreatedAndEmailSentResult(result: UserInvitationCreateResponse) {
        verify(exactly = 1) { mapper.toDomain(userInvitationCreateRequestBody) }

        // capture and verify the invitation that is saved by the service.
        val savedUserInvitationSlot = slot<UserInvitation>()
        verify(exactly = 1) { service.createUserInvitation(capture(savedUserInvitationSlot)) }
        val capturedUserInvitation = savedUserInvitationSlot.captured

        // make sure an invite code and pending status were set on the saved invitation
        Assertions.assertNotNull(capturedUserInvitation.inviteCode)
        Assertions.assertEquals(InvitationStatus.PENDING, capturedUserInvitation.status)

        // make sure an expiration time was set on the invitation
        Assertions.assertNotNull(capturedUserInvitation.expiresAt)

        // make sure the email sender was called with the correct inputs.
        val emailConfigSlot = slot<CustomerIoEmailConfig>()
        val inviteLinkSlot = slot<String>()

        verify(exactly = 1) {
          customerIoEmailNotificationSender.sendInviteToUser(
            capture(emailConfigSlot),
            currentUser.name,
            capture(inviteLinkSlot),
          )
        }

        val capturedEmailConfig = emailConfigSlot.captured
        Assertions.assertEquals(invitedEmail, capturedEmailConfig.to)

        val capturedInviteLink = inviteLinkSlot.captured
        Assertions.assertEquals(
          webappBaseUrl + UserInvitationHandler.ACCEPT_INVITE_PATH + capturedUserInvitation.inviteCode,
          capturedInviteLink,
        )

        // make sure we never created a permission, because the invitation path was taken instead.
        verify(exactly = 0) { permissionHandler.createPermission(any<Permission>()) }

        // make sure the final result is correct
        Assertions.assertFalse(result.directlyAdded)
        Assertions.assertEquals(capturedUserInvitation.inviteCode, result.inviteCode)

        // verify we sent an invitation tracking event
        verify(exactly = 1) {
          trackingClient.track(
            workspaceId,
            ScopeType.WORKSPACE,
            UserInvitationHandler.USER_INVITED,
            any<Map<String, Any?>>(),
          )
        }
      }
    }

    @Nested
    internal inner class DirectlyAddPermission {
      @BeforeEach
      fun setup() {
        every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false) } returns StandardWorkspace().withName(workspaceName)
      }

      @Test
      fun testExistingEmailInsideWorkspaceOrg() {
        every { workspaceService.getOrganizationIdFromWorkspaceId(workspaceId) } returns Optional.of(orgId)

        // set up user with the same email
        val matchingUserInOrg1 = User().withUserId(UUID.randomUUID()).withEmail(invitedEmail)
        every { userPersistence.getUserByEmail(invitedEmail) } returns Optional.of(matchingUserInOrg1)

        // set up two users inside the workspace's org, one with the same email and one with a different email.
        val otherUserInOrg = User().withUserId(UUID.randomUUID()).withEmail("other@airbyte.io")
        every { permissionHandler.listUsersInOrganization(orgId) } returns
          listOf(
            UserPermission().withUser(matchingUserInOrg1),
            UserPermission().withUser(otherUserInOrg),
          )
        every { permissionHandler.createPermission(any()) } returns mockk()
        every { customerIoEmailNotificationSender.sendNotificationOnInvitingExistingUser(any(), any(), any()) } returns Unit

        // call the handler method under test.
        val result: UserInvitationCreateResponse =
          handler.createInvitationOrPermission(
            userInvitationCreateRequestBody,
            currentUser,
          )

        // make sure permissions were created, appropriate email was sent, and result is correct.
        verifyPermissionAddedResult(setOf(matchingUserInOrg1.userId), result)
      }

      private fun verifyPermissionAddedResult(
        expectedUserIds: Set<UUID>,
        result: UserInvitationCreateResponse,
      ) {
        // capture and verify the permissions that are created by the permission handler.
        val permissionCreateSlot = mutableListOf<Permission>()
        verify(exactly = expectedUserIds.size) {
          permissionHandler.createPermission(capture(permissionCreateSlot))
        }

        // verify one captured permissionCreate per expected userId.
        Assertions.assertEquals(expectedUserIds.size, permissionCreateSlot.size)

        for (capturedPermissionCreate in permissionCreateSlot) {
          Assertions.assertEquals(workspaceId, capturedPermissionCreate.workspaceId)
          Assertions.assertEquals(Permission.PermissionType.WORKSPACE_ADMIN, capturedPermissionCreate.permissionType)
          Assertions.assertTrue(expectedUserIds.contains(capturedPermissionCreate.userId))
        }

        // make sure the email sender was called with the correct inputs.
        val emailConfigSlot = slot<CustomerIoEmailConfig>()
        verify(exactly = 1) {
          customerIoEmailNotificationSender.sendNotificationOnInvitingExistingUser(
            capture(emailConfigSlot),
            currentUser.name,
            workspaceName,
          )
        }

        Assertions.assertEquals(invitedEmail, emailConfigSlot.captured.to)

        // make sure we never created a user invitation, because the add-permission path was taken instead.
        verify(exactly = 0) { service.createUserInvitation(any<UserInvitation>()) }

        // make sure the final result is correct
        Assertions.assertTrue(result.directlyAdded)
        Assertions.assertNull(result.inviteCode)

        // we don't send a "user invited" event when a user is directly added to a workspace.
        verify(exactly = 0) {
          trackingClient.track(any<UUID>(), any<ScopeType>(), any<String>())
        }
      }
    }
  }

  @Nested
  internal inner class AcceptInvitation {
    private val inviteCode = "invite-code"
    private val inviteCodeRequestBody: InviteCodeRequestBody = InviteCodeRequestBody().inviteCode(inviteCode)
    private val currentUser: AuthenticatedUser = AuthenticatedUser().withUserId(UUID.randomUUID()).withEmail(CURRENT_USER_EMAIL)

    @ParameterizedTest
    @ValueSource(strings = [CURRENT_USER_EMAIL, CURRENT_USER_EMAIL_CASING])
    fun testEmailMatches(invitedEmail: String?) {
      val invitation =
        UserInvitation()
          .withInviteCode(inviteCode)
          .withInvitedEmail(invitedEmail)

      val invitationRead = mockk<UserInvitationRead>()

      every { service.getUserInvitationByInviteCode(inviteCode) } returns invitation
      every { service.acceptUserInvitation(inviteCode, currentUser.userId) } returns invitation
      every { mapper.toApi(invitation) } returns invitationRead

      val result = handler.accept(inviteCodeRequestBody, currentUser)

      verify(exactly = 1) { service.acceptUserInvitation(inviteCode, currentUser.userId) }

      // make sure the result is whatever the mapper outputs.
      Assertions.assertEquals(invitationRead, result)
    }

    @Test
    fun testEmailDoesNotMatch() {
      val invitation =
        UserInvitation()
          .withInviteCode(inviteCode)
          .withInvitedEmail("different@airbyte.io")

      every { service.getUserInvitationByInviteCode(inviteCode) } returns invitation

      Assertions.assertThrows(
        OperationNotAllowedException::class.java,
      ) { handler.accept(inviteCodeRequestBody, currentUser) }

      // make sure the service method to accept the invitation was never called.
      verify(exactly = 0) { service.acceptUserInvitation(any(), any()) }
    }

    @Test
    fun testInvitationStatusUnexpected() {
      val invitation =
        UserInvitation()
          .withInviteCode(inviteCode)
          .withInvitedEmail(CURRENT_USER_EMAIL)

      every { service.getUserInvitationByInviteCode(inviteCode) } returns invitation
      every { service.acceptUserInvitation(inviteCode, currentUser.userId) } answers { throw InvitationStatusUnexpectedException("not pending") }

      Assertions.assertThrows(
        ConflictException::class.java,
      ) { handler.accept(inviteCodeRequestBody, currentUser) }
    }

    @Test
    fun testInvitationExpired() {
      val invitation =
        UserInvitation()
          .withInviteCode(inviteCode)
          .withInvitedEmail(CURRENT_USER_EMAIL)

      every { service.getUserInvitationByInviteCode(inviteCode) } returns invitation
      every { service.acceptUserInvitation(inviteCode, currentUser.userId) } answers { throw InvitationStatusUnexpectedException("expired") }

      Assertions.assertThrows(
        ConflictException::class.java,
      ) { handler.accept(inviteCodeRequestBody, currentUser) }
    }
  }

  @Nested
  internal inner class CancelInvitation {
    @Test
    fun testCancelInvitationCallsService() {
      val inviteCode = "invite-code"
      val req = InviteCodeRequestBody().inviteCode(inviteCode)

      val cancelledInvitation =
        UserInvitation()
          .withInviteCode(inviteCode)
          .withInvitedEmail("invited@airbyte.io")
          .withStatus(InvitationStatus.CANCELLED)

      every { service.cancelUserInvitation(inviteCode) } returns cancelledInvitation
      every { mapper.toApi(cancelledInvitation) } returns mockk<UserInvitationRead>()

      handler.cancel(req)

      verify(exactly = 1) { service.cancelUserInvitation(inviteCode) }
    }

    @Test
    fun testCancelInvitationThrowsConflictExceptionOnUnexpectedStatus() {
      val inviteCode = "invite-code"
      val req = InviteCodeRequestBody().inviteCode(inviteCode)

      every { service.cancelUserInvitation(inviteCode) } answers { throw InvitationStatusUnexpectedException("unexpected status") }

      Assertions.assertThrows(
        ConflictException::class.java,
      ) { handler.cancel(req) }
    }
  }

  @Test
  fun pendingInvitationsTest() {
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val workspaceInvitations =
      listOf(
        mockk<UserInvitation>(),
        mockk<UserInvitation>(),
      )
    val organizationInvitations =
      listOf(
        mockk<UserInvitation>(),
        mockk<UserInvitation>(),
        mockk<UserInvitation>(),
      )

    every { service.getPendingInvitations(ScopeType.WORKSPACE, workspaceId) } returns workspaceInvitations
    every { service.getPendingInvitations(ScopeType.ORGANIZATION, organizationId) } returns organizationInvitations

    every { mapper.toDomain(io.airbyte.api.model.generated.ScopeType.WORKSPACE) } returns ScopeType.WORKSPACE
    every { mapper.toDomain(io.airbyte.api.model.generated.ScopeType.ORGANIZATION) } returns ScopeType.ORGANIZATION
    every { mapper.toApi(any<UserInvitation>()) } returns mockk<UserInvitationRead>()

    val workspaceResult =
      handler.getPendingInvitations(
        UserInvitationListRequestBody()
          .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE)
          .scopeId(workspaceId),
      )
    val organizationResult =
      handler.getPendingInvitations(
        UserInvitationListRequestBody()
          .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION)
          .scopeId(organizationId),
      )

    Assertions.assertEquals(workspaceInvitations.size, workspaceResult.size)
    Assertions.assertEquals(organizationInvitations.size, organizationResult.size)

    verify(exactly = 1) { service.getPendingInvitations(ScopeType.WORKSPACE, workspaceId) }
    verify(exactly = 1) { service.getPendingInvitations(ScopeType.ORGANIZATION, organizationId) }

    verify(exactly = workspaceInvitations.size + organizationInvitations.size) { mapper.toApi(any<UserInvitation>()) }
  }

  companion object {
    private const val CURRENT_USER_EMAIL = "current@airbyte.io"
    private const val CURRENT_USER_EMAIL_CASING = "cUrRenT@Airbyte.Io"
  }
}
