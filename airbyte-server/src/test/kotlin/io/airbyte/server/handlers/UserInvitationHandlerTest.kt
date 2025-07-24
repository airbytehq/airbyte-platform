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
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.eq
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import java.util.Optional
import java.util.UUID

@ExtendWith(MockitoExtension::class)
internal class UserInvitationHandlerTest {
  @Mock
  var service: UserInvitationService? = null

  @Mock
  var mapper: UserInvitationMapper? = null

  @Mock
  var customerIoEmailNotificationSender: CustomerIoEmailNotificationSender? = null

  @Mock
  var webUrlHelper: WebUrlHelper? = null

  @Mock
  var workspaceService: WorkspaceService? = null

  @Mock
  var organizationService: OrganizationService? = null

  @Mock
  var userPersistence: UserPersistence? = null

  @Mock
  var permissionHandler: PermissionHandler? = null

  @Mock
  var trackingClient: TrackingClient? = null

  var handler: UserInvitationHandler? = null

  @BeforeEach
  fun setup() {
    handler =
      UserInvitationHandler(
        service!!,
        mapper!!,
        customerIoEmailNotificationSender!!,
        webUrlHelper!!,
        workspaceService!!,
        organizationService!!,
        userPersistence!!,
        permissionHandler!!,
        trackingClient!!,
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
      @Throws(Exception::class)
      private fun setupSendInvitationMocks() {
        Mockito.`when`<String>(webUrlHelper!!.baseUrl).thenReturn(webappBaseUrl)
        Mockito.`when`<UserInvitation>(service!!.createUserInvitation(userInvitation)).thenReturn(userInvitation)
        Mockito.`when`<StandardWorkspace>(workspaceService!!.getStandardWorkspaceNoSecrets(workspaceId, false)).thenReturn(
          StandardWorkspace().withName(
            workspaceName,
          ),
        )
      }

      @BeforeEach
      fun setup() {
        Mockito.`when`<UserInvitation>(mapper!!.toDomain(userInvitationCreateRequestBody)).thenReturn(
          userInvitation,
        )
      }

      @Test
      @Throws(Exception::class)
      fun testNewEmailWorkspaceInOrg() {
        setupSendInvitationMocks()

        // the workspace is in an org.
        Mockito.`when`<Optional<UUID>>(workspaceService!!.getOrganizationIdFromWorkspaceId(workspaceId)).thenReturn(
          Optional.of<UUID>(
            orgId,
          ),
        )

        // no existing user has the invited email.
        Mockito.`when`<Optional<User>>(userPersistence!!.getUserByEmail(invitedEmail)).thenReturn(Optional.empty<User>())

        // call the handler method under test.
        val result: UserInvitationCreateResponse =
          handler!!.createInvitationOrPermission(
            userInvitationCreateRequestBody,
            currentUser,
          )

        // make sure correct invite was created, email was sent, and result is correct.
        verifyInvitationCreatedAndEmailSentResult(result)
      }

      @Test
      @Throws(Exception::class)
      fun testWorkspaceNotInAnyOrg() {
        setupSendInvitationMocks()

        // the workspace is not in any org.
        Mockito.`when`<Optional<UUID>>(workspaceService!!.getOrganizationIdFromWorkspaceId(workspaceId)).thenReturn(Optional.empty<UUID>())

        // call the handler method under test.
        val result: UserInvitationCreateResponse =
          handler!!.createInvitationOrPermission(
            userInvitationCreateRequestBody,
            currentUser,
          )

        // make sure correct invite was created, email was sent, and result is correct.
        verifyInvitationCreatedAndEmailSentResult(result)
      }

      @Test
      @Throws(Exception::class)
      fun testExistingEmailButNotInWorkspaceOrg() {
        setupSendInvitationMocks()

        // the workspace is in an org.
        Mockito.`when`<Optional<UUID>>(workspaceService!!.getOrganizationIdFromWorkspaceId(workspaceId)).thenReturn(
          Optional.of<UUID>(
            orgId,
          ),
        )

        // a user with the email exists, but is not in the workspace's org.
        val userWithEmail = User().withUserId(UUID.randomUUID()).withEmail(invitedEmail)
        Mockito.`when`<Optional<User>>(userPersistence!!.getUserByEmail(invitedEmail)).thenReturn(Optional.of<User>(userWithEmail))

        // the org has a user with a different email, but not the one we're inviting.
        val otherUserInOrg = User().withUserId(UUID.randomUUID()).withEmail("other@airbyte.io")
        Mockito
          .`when`<List<UserPermission>>(permissionHandler!!.listUsersInOrganization(orgId))
          .thenReturn(java.util.List.of<UserPermission>(UserPermission().withUser(otherUserInOrg)))

        // call the handler method under test.
        val result: UserInvitationCreateResponse =
          handler!!.createInvitationOrPermission(
            userInvitationCreateRequestBody,
            currentUser,
          )

        // make sure correct invite was created, email was sent, and result is correct.
        verifyInvitationCreatedAndEmailSentResult(result)
      }

      @Test
      @Throws(Exception::class)
      fun testThrowsConflictExceptionOnDuplicateInvitation() {
        Mockito.`when`<UserInvitation>(service!!.createUserInvitation(userInvitation)).thenThrow(InvitationDuplicateException("duplicate"))

        Assertions.assertThrows<ConflictException>(
          ConflictException::class.java,
        ) {
          handler!!.createInvitationOrPermission(
            userInvitationCreateRequestBody,
            currentUser,
          )
        }
      }

      private fun verifyInvitationCreatedAndEmailSentResult(result: UserInvitationCreateResponse) {
        verify(mapper!!, Mockito.times(1)).toDomain(userInvitationCreateRequestBody)

        // capture and verify the invitation that is saved by the service.
        val savedUserInvitationCaptor = org.mockito.kotlin.argumentCaptor<UserInvitation>()
        verify(service!!, Mockito.times(1)).createUserInvitation(savedUserInvitationCaptor.capture())
        val capturedUserInvitation = savedUserInvitationCaptor.firstValue

        // make sure an invite code and pending status were set on the saved invitation
        Assertions.assertNotNull(capturedUserInvitation.inviteCode)
        Assertions.assertEquals(InvitationStatus.PENDING, capturedUserInvitation.status)

        // make sure an expiration time was set on the invitation
        Assertions.assertNotNull(capturedUserInvitation.expiresAt)

        // make sure the email sender was called with the correct inputs.
        val emailConfigCaptor = org.mockito.kotlin.argumentCaptor<CustomerIoEmailConfig>()
        val inviteLinkCaptor = org.mockito.kotlin.argumentCaptor<String>()

        verify(customerIoEmailNotificationSender!!, Mockito.times(1)).sendInviteToUser(
          emailConfigCaptor.capture(),
          eq(currentUser.name),
          inviteLinkCaptor.capture(),
        )

        val capturedEmailConfig = emailConfigCaptor.firstValue
        Assertions.assertEquals(invitedEmail, capturedEmailConfig.to)

        val capturedInviteLink = inviteLinkCaptor.firstValue
        Assertions.assertEquals(
          webappBaseUrl + UserInvitationHandler.ACCEPT_INVITE_PATH + capturedUserInvitation.inviteCode,
          capturedInviteLink,
        )

        // make sure no other emails are sent.
        Mockito.verifyNoMoreInteractions(customerIoEmailNotificationSender)

        // make sure we never created a permission, because the invitation path was taken instead.
        verify(permissionHandler!!, Mockito.times(0)).createPermission(org.mockito.kotlin.any<Permission>())

        // make sure the final result is correct
        Assertions.assertFalse(result.directlyAdded)
        Assertions.assertEquals(capturedUserInvitation.inviteCode, result.inviteCode)

        // verify we sent an invitation tracking event
        verify(trackingClient!!, Mockito.times(1)).track(
          eq(workspaceId),
          eq(ScopeType.WORKSPACE),
          eq(UserInvitationHandler.USER_INVITED),
          org.mockito.kotlin.any<Map<String, Any?>>(),
        )
      }
    }

    @Nested
    internal inner class DirectlyAddPermission {
      @BeforeEach
      fun setup() {
        Mockito.`when`<StandardWorkspace>(workspaceService!!.getStandardWorkspaceNoSecrets(workspaceId, false)).thenReturn(
          StandardWorkspace().withName(
            workspaceName,
          ),
        )
      }

      @Test
      fun testExistingEmailInsideWorkspaceOrg() {
        Mockito
          .`when`(workspaceService!!.getOrganizationIdFromWorkspaceId(workspaceId))
          .thenReturn(Optional.of(orgId))

        // set up user with the same email
        val matchingUserInOrg1 = User().withUserId(UUID.randomUUID()).withEmail(invitedEmail)
        Mockito
          .`when`(userPersistence!!.getUserByEmail(invitedEmail))
          .thenReturn(Optional.of(matchingUserInOrg1))

        // set up two users inside the workspace's org, one with the same email and one with a different email.
        val otherUserInOrg = User().withUserId(UUID.randomUUID()).withEmail("other@airbyte.io")
        Mockito.`when`(permissionHandler!!.listUsersInOrganization(orgId)).thenReturn(
          listOf(
            UserPermission().withUser(matchingUserInOrg1),
            UserPermission().withUser(otherUserInOrg),
          ),
        )

        // call the handler method under test.
        val result: UserInvitationCreateResponse =
          handler!!.createInvitationOrPermission(
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
        // capture and verify the permissions that are created by the permission handler!!.
        val permissionCreateCaptor =
          org.mockito.kotlin.argumentCaptor<Permission>()
        verify(permissionHandler!!, Mockito.times(expectedUserIds.size))
          .createPermission(permissionCreateCaptor.capture())
        Mockito.verifyNoMoreInteractions(permissionHandler)

        // verify one captured permissionCreate per expected userId.
        val capturedPermissionCreateValues = permissionCreateCaptor.allValues
        Assertions.assertEquals(expectedUserIds.size, capturedPermissionCreateValues.size)

        for (capturedPermissionCreate in capturedPermissionCreateValues) {
          Assertions.assertEquals(workspaceId, capturedPermissionCreate.workspaceId)
          Assertions.assertEquals(Permission.PermissionType.WORKSPACE_ADMIN, capturedPermissionCreate.permissionType)
          Assertions.assertTrue(expectedUserIds.contains(capturedPermissionCreate.userId))
        }

        // make sure the email sender was called with the correct inputs.
        val emailConfigCaptor = org.mockito.kotlin.argumentCaptor<CustomerIoEmailConfig>()
        verify(customerIoEmailNotificationSender!!, Mockito.times(1))
          .sendNotificationOnInvitingExistingUser(
            emailConfigCaptor.capture(),
            eq<String>(currentUser.name),
            eq<String>(workspaceName),
          )

        Assertions.assertEquals(invitedEmail, emailConfigCaptor.firstValue.to)

        // make sure no other emails are sent.
        Mockito.verifyNoMoreInteractions(customerIoEmailNotificationSender)

        // make sure we never created a user invitation, because the add-permission path was taken instead.
        verify(service!!, Mockito.times(0)).createUserInvitation(org.mockito.kotlin.any<UserInvitation>())

        // make sure the final result is correct
        Assertions.assertTrue(result.directlyAdded)
        Assertions.assertNull(result.inviteCode)

        // we don't send a "user invited" event when a user is directly added to a workspace.
        verify(trackingClient!!, Mockito.never())
          .track(org.mockito.kotlin.any<UUID>(), org.mockito.kotlin.any<ScopeType>(), org.mockito.kotlin.any<String>())
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

      val invitationRead =
        Mockito.mock(
          UserInvitationRead::class.java,
        )

      Mockito.`when`(service!!.getUserInvitationByInviteCode(inviteCode)).thenReturn(invitation)
      Mockito
        .`when`(service!!.acceptUserInvitation(inviteCode, currentUser.userId))
        .thenReturn(invitation)

      Mockito.`when`(mapper!!.toApi(invitation)).thenReturn(invitationRead)

      val result = handler!!.accept(inviteCodeRequestBody, currentUser)

      verify(service!!, Mockito.times(1)).acceptUserInvitation(inviteCode, currentUser.userId)
      Mockito.verifyNoMoreInteractions(service)

      // make sure the result is whatever the mapper outputs.
      Assertions.assertEquals(invitationRead, result)
    }

    @Test
    @Throws(Exception::class)
    fun testEmailDoesNotMatch() {
      val invitation =
        UserInvitation()
          .withInviteCode(inviteCode)
          .withInvitedEmail("different@airbyte.io")

      Mockito.`when`(service!!.getUserInvitationByInviteCode(inviteCode)).thenReturn(invitation)

      Assertions.assertThrows(
        OperationNotAllowedException::class.java,
      ) { handler!!.accept(inviteCodeRequestBody, currentUser) }

      // make sure the service method to accept the invitation was never called.
      verify(service!!, Mockito.times(0)).acceptUserInvitation(org.mockito.kotlin.any(), org.mockito.kotlin.any())
    }

    @Test
    @Throws(Exception::class)
    fun testInvitationStatusUnexpected() {
      val invitation =
        UserInvitation()
          .withInviteCode(inviteCode)
          .withInvitedEmail(CURRENT_USER_EMAIL)

      Mockito.`when`(service!!.getUserInvitationByInviteCode(inviteCode)).thenReturn(invitation)

      Mockito
        .doThrow(InvitationStatusUnexpectedException("not pending"))
        .`when`(service!!)
        .acceptUserInvitation(inviteCode, currentUser.userId)

      Assertions.assertThrows(
        ConflictException::class.java,
      ) { handler!!.accept(inviteCodeRequestBody, currentUser) }
    }

    @Test
    @Throws(Exception::class)
    fun testInvitationExpired() {
      val invitation =
        UserInvitation()
          .withInviteCode(inviteCode)
          .withInvitedEmail(CURRENT_USER_EMAIL)

      Mockito.`when`(service!!.getUserInvitationByInviteCode(inviteCode)).thenReturn(invitation)
      Mockito
        .doThrow(InvitationStatusUnexpectedException("expired"))
        .`when`(service!!)
        .acceptUserInvitation(inviteCode, currentUser.userId)

      Assertions.assertThrows(
        ConflictException::class.java,
      ) { handler!!.accept(inviteCodeRequestBody, currentUser) }
    }
  }

  @Nested
  internal inner class CancelInvitation {
    @Test
    @Throws(Exception::class)
    fun testCancelInvitationCallsService() {
      val inviteCode = "invite-code"
      val req = InviteCodeRequestBody().inviteCode(inviteCode)

      val cancelledInvitation =
        UserInvitation()
          .withInviteCode(inviteCode)
          .withInvitedEmail("invited@airbyte.io")
          .withStatus(InvitationStatus.CANCELLED)

      Mockito.`when`(service!!.cancelUserInvitation(inviteCode)).thenReturn(cancelledInvitation)
      Mockito.`when`(mapper!!.toApi(cancelledInvitation)).thenReturn(
        Mockito.mock(
          UserInvitationRead::class.java,
        ),
      )

      handler!!.cancel(req)

      verify(service!!, Mockito.times(1)).cancelUserInvitation(inviteCode)
      Mockito.verifyNoMoreInteractions(service)
    }

    @Test
    @Throws(Exception::class)
    fun testCancelInvitationThrowsConflictExceptionOnUnexpectedStatus() {
      val inviteCode = "invite-code"
      val req = InviteCodeRequestBody().inviteCode(inviteCode)

      Mockito.`when`(service!!.cancelUserInvitation(inviteCode)).thenThrow(InvitationStatusUnexpectedException("unexpected status"))

      Assertions.assertThrows(
        ConflictException::class.java,
      ) { handler!!.cancel(req) }
    }
  }

  @Test
  fun pendingInvitationsTest() {
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val workspaceInvitations =
      java.util.List.of(
        Mockito.mock(
          UserInvitation::class.java,
        ),
        Mockito.mock(UserInvitation::class.java),
      )
    val organizationInvitations =
      java.util.List.of(
        Mockito.mock(
          UserInvitation::class.java,
        ),
        Mockito.mock(UserInvitation::class.java),
        Mockito.mock(
          UserInvitation::class.java,
        ),
      )

    Mockito.`when`(service!!.getPendingInvitations(ScopeType.WORKSPACE, workspaceId)).thenReturn(workspaceInvitations)
    Mockito.`when`(service!!.getPendingInvitations(ScopeType.ORGANIZATION, organizationId)).thenReturn(organizationInvitations)

    Mockito.`when`(mapper!!.toDomain(io.airbyte.api.model.generated.ScopeType.WORKSPACE)).thenReturn(ScopeType.WORKSPACE)
    Mockito.`when`(mapper!!.toDomain(io.airbyte.api.model.generated.ScopeType.ORGANIZATION)).thenReturn(ScopeType.ORGANIZATION)
    Mockito
      .`when`(
        mapper!!.toApi(org.mockito.kotlin.any<UserInvitation>()),
      ).thenReturn(Mockito.mock(UserInvitationRead::class.java))

    val workspaceResult =
      handler!!.getPendingInvitations(
        UserInvitationListRequestBody()
          .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE)
          .scopeId(workspaceId),
      )
    val organizationResult =
      handler!!.getPendingInvitations(
        UserInvitationListRequestBody()
          .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION)
          .scopeId(organizationId),
      )

    Assertions.assertEquals(workspaceInvitations.size, workspaceResult.size)
    Assertions.assertEquals(organizationInvitations.size, organizationResult.size)

    verify(service!!, Mockito.times(1)).getPendingInvitations(ScopeType.WORKSPACE, workspaceId)
    verify(service!!, Mockito.times(1)).getPendingInvitations(ScopeType.ORGANIZATION, organizationId)

    verify(mapper, Mockito.times(workspaceInvitations.size + organizationInvitations.size))?.toApi(org.mockito.kotlin.any<UserInvitation>())
  }

  companion object {
    private const val CURRENT_USER_EMAIL = "current@airbyte.io"
    private const val CURRENT_USER_EMAIL_CASING = "cUrRenT@Airbyte.Io"
  }
}
