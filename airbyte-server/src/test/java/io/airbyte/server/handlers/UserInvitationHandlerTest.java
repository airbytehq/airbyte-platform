/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers;

import static io.airbyte.config.Permission.PermissionType.WORKSPACE_ADMIN;
import static io.airbyte.server.handlers.UserInvitationHandler.ACCEPT_INVITE_PATH;
import static io.airbyte.server.handlers.UserInvitationHandler.USER_INVITED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.airbyte.analytics.TrackingClient;
import io.airbyte.api.model.generated.InviteCodeRequestBody;
import io.airbyte.api.model.generated.PermissionCreate;
import io.airbyte.api.model.generated.PermissionType;
import io.airbyte.api.model.generated.UserInvitationCreateRequestBody;
import io.airbyte.api.model.generated.UserInvitationCreateResponse;
import io.airbyte.api.model.generated.UserInvitationListRequestBody;
import io.airbyte.api.model.generated.UserInvitationRead;
import io.airbyte.commons.server.errors.ConflictException;
import io.airbyte.commons.server.errors.OperationNotAllowedException;
import io.airbyte.commons.server.handlers.PermissionHandler;
import io.airbyte.config.AuthenticatedUser;
import io.airbyte.config.InvitationStatus;
import io.airbyte.config.ScopeType;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.User;
import io.airbyte.config.UserInvitation;
import io.airbyte.config.UserPermission;
import io.airbyte.config.persistence.PermissionPersistence;
import io.airbyte.config.persistence.UserPersistence;
import io.airbyte.data.services.InvitationDuplicateException;
import io.airbyte.data.services.InvitationStatusUnexpectedException;
import io.airbyte.data.services.OrganizationService;
import io.airbyte.data.services.UserInvitationService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.notification.CustomerIoEmailConfig;
import io.airbyte.notification.CustomerIoEmailNotificationSender;
import io.airbyte.persistence.job.WebUrlHelper;
import io.airbyte.server.handlers.api_domain_mapping.UserInvitationMapper;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class UserInvitationHandlerTest {

  @Mock
  UserInvitationService service;
  @Mock
  UserInvitationMapper mapper;
  @Mock
  CustomerIoEmailNotificationSender customerIoEmailNotificationSender;
  @Mock
  WebUrlHelper webUrlHelper;
  @Mock
  WorkspaceService workspaceService;
  @Mock
  OrganizationService organizationService;
  @Mock
  UserPersistence userPersistence;
  @Mock
  PermissionPersistence permissionPersistence;
  @Mock
  PermissionHandler permissionHandler;
  @Mock
  TrackingClient trackingClient;

  UserInvitationHandler handler;

  @BeforeEach
  void setup() {
    handler = new UserInvitationHandler(service, mapper, customerIoEmailNotificationSender, webUrlHelper, workspaceService, organizationService,
        userPersistence, permissionPersistence, permissionHandler, trackingClient);
  }

  @Nested
  class CreateInvitationOrPermission {

    private static final AuthenticatedUser CURRENT_USER =
        new AuthenticatedUser().withUserId(UUID.randomUUID()).withEmail("current-user@airbyte.io").withName("Current User");
    private static final String WEBAPP_BASE_URL = "https://test.airbyte.io";
    private static final String INVITED_EMAIL = "invited@airbyte.io";
    private static final UUID WORKSPACE_ID = UUID.randomUUID();
    private static final String WORKSPACE_NAME = "workspace-name";
    private static final UUID ORG_ID = UUID.randomUUID();
    private static final UserInvitationCreateRequestBody USER_INVITATION_CREATE_REQUEST_BODY = new UserInvitationCreateRequestBody()
        .invitedEmail(INVITED_EMAIL)
        .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE)
        .scopeId(WORKSPACE_ID)
        .permissionType(io.airbyte.api.model.generated.PermissionType.WORKSPACE_ADMIN);

    @Nested
    class CreateAndSendInvitation {

      private static final UserInvitation USER_INVITATION = new UserInvitation()
          .withInvitedEmail(INVITED_EMAIL)
          .withScopeType(ScopeType.WORKSPACE)
          .withScopeId(WORKSPACE_ID)
          .withPermissionType(WORKSPACE_ADMIN);

      private void setupSendInvitationMocks() throws Exception {
        when(webUrlHelper.getBaseUrl()).thenReturn(WEBAPP_BASE_URL);
        when(service.createUserInvitation(USER_INVITATION)).thenReturn(USER_INVITATION);
        when(workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, false)).thenReturn(new StandardWorkspace().withName(WORKSPACE_NAME));
      }

      @BeforeEach
      void setup() {
        when(mapper.toDomain(USER_INVITATION_CREATE_REQUEST_BODY)).thenReturn(USER_INVITATION);
      }

      @Test
      void testNewEmailWorkspaceInOrg() throws Exception {
        setupSendInvitationMocks();

        // the workspace is in an org.
        when(workspaceService.getOrganizationIdFromWorkspaceId(WORKSPACE_ID)).thenReturn(Optional.of(ORG_ID));

        // no existing user has the invited email.
        when(userPersistence.getUserByEmail(INVITED_EMAIL)).thenReturn(Optional.empty());

        // call the handler method under test.
        final UserInvitationCreateResponse result = handler.createInvitationOrPermission(USER_INVITATION_CREATE_REQUEST_BODY, CURRENT_USER);

        // make sure correct invite was created, email was sent, and result is correct.
        verifyInvitationCreatedAndEmailSentResult(result);
      }

      @Test
      void testWorkspaceNotInAnyOrg() throws Exception {
        setupSendInvitationMocks();

        // the workspace is not in any org.
        when(workspaceService.getOrganizationIdFromWorkspaceId(WORKSPACE_ID)).thenReturn(Optional.empty());

        // call the handler method under test.
        final UserInvitationCreateResponse result = handler.createInvitationOrPermission(USER_INVITATION_CREATE_REQUEST_BODY, CURRENT_USER);

        // make sure correct invite was created, email was sent, and result is correct.
        verifyInvitationCreatedAndEmailSentResult(result);
      }

      @Test
      void testExistingEmailButNotInWorkspaceOrg() throws Exception {
        setupSendInvitationMocks();

        // the workspace is in an org.
        when(workspaceService.getOrganizationIdFromWorkspaceId(WORKSPACE_ID)).thenReturn(Optional.of(ORG_ID));

        // a user with the email exists, but is not in the workspace's org.
        final User userWithEmail = new User().withUserId(UUID.randomUUID()).withEmail(INVITED_EMAIL);
        when(userPersistence.getUserByEmail(INVITED_EMAIL)).thenReturn(Optional.of(userWithEmail));

        // the org has a user with a different email, but not the one we're inviting.
        final User otherUserInOrg = new User().withUserId(UUID.randomUUID()).withEmail("other@airbyte.io");
        when(permissionPersistence.listUsersInOrganization(ORG_ID)).thenReturn(List.of(new UserPermission().withUser(otherUserInOrg)));

        // call the handler method under test.
        final UserInvitationCreateResponse result = handler.createInvitationOrPermission(USER_INVITATION_CREATE_REQUEST_BODY, CURRENT_USER);

        // make sure correct invite was created, email was sent, and result is correct.
        verifyInvitationCreatedAndEmailSentResult(result);
      }

      @Test
      void testThrowsConflictExceptionOnDuplicateInvitation() throws Exception {
        when(service.createUserInvitation(USER_INVITATION)).thenThrow(new InvitationDuplicateException("duplicate"));

        assertThrows(ConflictException.class, () -> handler.createInvitationOrPermission(USER_INVITATION_CREATE_REQUEST_BODY, CURRENT_USER));
      }

      private void verifyInvitationCreatedAndEmailSentResult(final UserInvitationCreateResponse result) throws Exception {
        verify(mapper, times(1)).toDomain(USER_INVITATION_CREATE_REQUEST_BODY);

        // capture and verify the invitation that is saved by the service.
        final ArgumentCaptor<UserInvitation> savedUserInvitationCaptor = ArgumentCaptor.forClass(UserInvitation.class);
        verify(service, times(1)).createUserInvitation(savedUserInvitationCaptor.capture());
        final UserInvitation capturedUserInvitation = savedUserInvitationCaptor.getValue();

        // make sure an invite code and pending status were set on the saved invitation
        assertNotNull(capturedUserInvitation.getInviteCode());
        assertEquals(InvitationStatus.PENDING, capturedUserInvitation.getStatus());

        // make sure an expiration time was set on the invitation
        assertNotNull(capturedUserInvitation.getExpiresAt());

        // make sure the email sender was called with the correct inputs.
        final ArgumentCaptor<CustomerIoEmailConfig> emailConfigCaptor = ArgumentCaptor.forClass(CustomerIoEmailConfig.class);
        final ArgumentCaptor<String> inviteLinkCaptor = ArgumentCaptor.forClass(String.class);

        verify(customerIoEmailNotificationSender, times(1)).sendInviteToUser(
            emailConfigCaptor.capture(),
            eq(CURRENT_USER.getName()),
            inviteLinkCaptor.capture());

        final CustomerIoEmailConfig capturedEmailConfig = emailConfigCaptor.getValue();
        assertEquals(INVITED_EMAIL, capturedEmailConfig.getTo());

        final String capturedInviteLink = inviteLinkCaptor.getValue();
        assertEquals(WEBAPP_BASE_URL + ACCEPT_INVITE_PATH + capturedUserInvitation.getInviteCode(), capturedInviteLink);

        // make sure no other emails are sent.
        verifyNoMoreInteractions(customerIoEmailNotificationSender);

        // make sure we never created a permission, because the invitation path was taken instead.
        verify(permissionHandler, times(0)).createPermission(any());

        // make sure the final result is correct
        assertFalse(result.getDirectlyAdded());
        assertEquals(capturedUserInvitation.getInviteCode(), result.getInviteCode());

        // verify we sent an invitation tracking event
        verify(trackingClient, times(1)).track(
            eq(WORKSPACE_ID),
            eq(ScopeType.WORKSPACE),
            eq(USER_INVITED),
            anyMap());
      }

    }

    @Nested
    class DirectlyAddPermission {

      @BeforeEach
      void setup() throws Exception {
        when(workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, false)).thenReturn(new StandardWorkspace().withName(WORKSPACE_NAME));
      }

      @Test
      void testExistingEmailInsideWorkspaceOrg() throws Exception {
        when(workspaceService.getOrganizationIdFromWorkspaceId(WORKSPACE_ID)).thenReturn(Optional.of(ORG_ID));

        // set up user with the same email
        final User matchingUserInOrg1 = new User().withUserId(UUID.randomUUID()).withEmail(INVITED_EMAIL);
        when(userPersistence.getUserByEmail(INVITED_EMAIL))
            .thenReturn(Optional.of(matchingUserInOrg1));

        // set up two users inside the workspace's org, one with the same email and one with a different
        // email.
        final User otherUserInOrg = new User().withUserId(UUID.randomUUID()).withEmail("other@airbyte.io");
        when(permissionPersistence.listUsersInOrganization(ORG_ID)).thenReturn(List.of(
            new UserPermission().withUser(matchingUserInOrg1),
            new UserPermission().withUser(otherUserInOrg)));

        // call the handler method under test.
        final UserInvitationCreateResponse result = handler.createInvitationOrPermission(USER_INVITATION_CREATE_REQUEST_BODY, CURRENT_USER);

        // make sure permissions were created, appropriate email was sent, and result is correct.
        verifyPermissionAddedResult(Set.of(matchingUserInOrg1.getUserId()), result);
      }

      private void verifyPermissionAddedResult(final Set<UUID> expectedUserIds, final UserInvitationCreateResponse result) throws Exception {
        // capture and verify the permissions that are created by the permission handler.
        final ArgumentCaptor<PermissionCreate> permissionCreateCaptor = ArgumentCaptor.forClass(PermissionCreate.class);
        verify(permissionHandler, times(expectedUserIds.size())).createPermission(permissionCreateCaptor.capture());
        verifyNoMoreInteractions(permissionHandler);

        // verify one captured permissionCreate per expected userId.
        final List<PermissionCreate> capturedPermissionCreateValues = permissionCreateCaptor.getAllValues();
        assertEquals(expectedUserIds.size(), capturedPermissionCreateValues.size());

        for (final PermissionCreate capturedPermissionCreate : capturedPermissionCreateValues) {
          assertEquals(WORKSPACE_ID, capturedPermissionCreate.getWorkspaceId());
          assertEquals(PermissionType.WORKSPACE_ADMIN, capturedPermissionCreate.getPermissionType());
          assertTrue(expectedUserIds.contains(capturedPermissionCreate.getUserId()));
        }

        // make sure the email sender was called with the correct inputs.
        final ArgumentCaptor<CustomerIoEmailConfig> emailConfigCaptor = ArgumentCaptor.forClass(CustomerIoEmailConfig.class);
        verify(customerIoEmailNotificationSender, times(1)).sendNotificationOnInvitingExistingUser(
            emailConfigCaptor.capture(),
            eq(CURRENT_USER.getName()),
            eq(WORKSPACE_NAME));

        assertEquals(INVITED_EMAIL, emailConfigCaptor.getValue().getTo());

        // make sure no other emails are sent.
        verifyNoMoreInteractions(customerIoEmailNotificationSender);

        // make sure we never created a user invitation, because the add-permission path was taken instead.
        verify(service, times(0)).createUserInvitation(any());

        // make sure the final result is correct
        assertTrue(result.getDirectlyAdded());
        assertNull(result.getInviteCode());

        // we don't send a "user invited" event when a user is directly added to a workspace.
        verify(trackingClient, never()).track(any(), any(), any());
      }

    }

  }

  @Nested
  class AcceptInvitation {

    private static final String INVITE_CODE = "invite-code";
    private static final InviteCodeRequestBody INVITE_CODE_REQUEST_BODY = new InviteCodeRequestBody().inviteCode(INVITE_CODE);
    private static final String CURRENT_USER_EMAIL = "current@airbyte.io";
    private static final String CURRENT_USER_EMAIL_CASING = "cUrRenT@Airbyte.Io";
    private static final AuthenticatedUser CURRENT_USER = new AuthenticatedUser().withUserId(UUID.randomUUID()).withEmail(CURRENT_USER_EMAIL);

    @ParameterizedTest
    @ValueSource(strings = {CURRENT_USER_EMAIL, CURRENT_USER_EMAIL_CASING})
    void testEmailMatches(final String invitedEmail) throws Exception {
      final UserInvitation invitation = new UserInvitation()
          .withInviteCode(INVITE_CODE)
          .withInvitedEmail(invitedEmail);

      final UserInvitationRead invitationRead = mock(UserInvitationRead.class);

      when(service.getUserInvitationByInviteCode(INVITE_CODE)).thenReturn(invitation);
      when(service.acceptUserInvitation(INVITE_CODE, CURRENT_USER.getUserId()))
          .thenReturn(invitation);

      when(mapper.toApi(invitation)).thenReturn(invitationRead);

      final UserInvitationRead result = handler.accept(INVITE_CODE_REQUEST_BODY, CURRENT_USER);

      verify(service, times(1)).acceptUserInvitation(INVITE_CODE, CURRENT_USER.getUserId());
      verifyNoMoreInteractions(service);

      // make sure the result is whatever the mapper outputs.
      assertEquals(invitationRead, result);
    }

    @Test
    void testEmailDoesNotMatch() throws Exception {
      final UserInvitation invitation = new UserInvitation()
          .withInviteCode(INVITE_CODE)
          .withInvitedEmail("different@airbyte.io");

      when(service.getUserInvitationByInviteCode(INVITE_CODE)).thenReturn(invitation);

      assertThrows(OperationNotAllowedException.class, () -> handler.accept(INVITE_CODE_REQUEST_BODY, CURRENT_USER));

      // make sure the service method to accept the invitation was never called.
      verify(service, times(0)).acceptUserInvitation(any(), any());
    }

    @Test
    void testInvitationStatusUnexpected() throws Exception {
      final UserInvitation invitation = new UserInvitation()
          .withInviteCode(INVITE_CODE)
          .withInvitedEmail(CURRENT_USER_EMAIL);

      when(service.getUserInvitationByInviteCode(INVITE_CODE)).thenReturn(invitation);

      doThrow(new InvitationStatusUnexpectedException("not pending"))
          .when(service).acceptUserInvitation(INVITE_CODE, CURRENT_USER.getUserId());

      assertThrows(ConflictException.class, () -> handler.accept(INVITE_CODE_REQUEST_BODY, CURRENT_USER));
    }

    @Test
    void testInvitationExpired() throws Exception {
      final UserInvitation invitation = new UserInvitation()
          .withInviteCode(INVITE_CODE)
          .withInvitedEmail(CURRENT_USER_EMAIL);

      when(service.getUserInvitationByInviteCode(INVITE_CODE)).thenReturn(invitation);
      doThrow(new InvitationStatusUnexpectedException("expired"))
          .when(service).acceptUserInvitation(INVITE_CODE, CURRENT_USER.getUserId());

      assertThrows(ConflictException.class, () -> handler.accept(INVITE_CODE_REQUEST_BODY, CURRENT_USER));
    }

  }

  @Nested
  class CancelInvitation {

    @Test
    void testCancelInvitationCallsService() throws Exception {
      final String inviteCode = "invite-code";
      final InviteCodeRequestBody req = new InviteCodeRequestBody().inviteCode(inviteCode);

      final UserInvitation cancelledInvitation = new UserInvitation()
          .withInviteCode(inviteCode)
          .withInvitedEmail("invited@airbyte.io")
          .withStatus(InvitationStatus.CANCELLED);

      when(service.cancelUserInvitation(inviteCode)).thenReturn(cancelledInvitation);
      when(mapper.toApi(cancelledInvitation)).thenReturn(mock(UserInvitationRead.class));

      handler.cancel(req);

      verify(service, times(1)).cancelUserInvitation(inviteCode);
      verifyNoMoreInteractions(service);
    }

    @Test
    void testCancelInvitationThrowsConflictExceptionOnUnexpectedStatus() throws Exception {
      final String inviteCode = "invite-code";
      final InviteCodeRequestBody req = new InviteCodeRequestBody().inviteCode(inviteCode);

      when(service.cancelUserInvitation(inviteCode)).thenThrow(new InvitationStatusUnexpectedException("unexpected status"));

      assertThrows(ConflictException.class, () -> handler.cancel(req));
    }

  }

  @Test
  void getPendingInvitationsTest() {
    final UUID workspaceId = UUID.randomUUID();
    final UUID organizationId = UUID.randomUUID();
    final List<UserInvitation> workspaceInvitations = List.of(mock(UserInvitation.class), mock(UserInvitation.class));
    final List<UserInvitation> organizationInvitations = List.of(mock(UserInvitation.class), mock(UserInvitation.class), mock(UserInvitation.class));

    when(service.getPendingInvitations(ScopeType.WORKSPACE, workspaceId)).thenReturn(workspaceInvitations);
    when(service.getPendingInvitations(ScopeType.ORGANIZATION, organizationId)).thenReturn(organizationInvitations);

    when(mapper.toDomain(io.airbyte.api.model.generated.ScopeType.WORKSPACE)).thenReturn(ScopeType.WORKSPACE);
    when(mapper.toDomain(io.airbyte.api.model.generated.ScopeType.ORGANIZATION)).thenReturn(ScopeType.ORGANIZATION);
    when(mapper.toApi(any(UserInvitation.class))).thenReturn(mock(UserInvitationRead.class));

    final List<UserInvitationRead> workspaceResult = handler.getPendingInvitations(new UserInvitationListRequestBody()
        .scopeType(io.airbyte.api.model.generated.ScopeType.WORKSPACE)
        .scopeId(workspaceId));
    final List<UserInvitationRead> organizationResult = handler.getPendingInvitations(new UserInvitationListRequestBody()
        .scopeType(io.airbyte.api.model.generated.ScopeType.ORGANIZATION)
        .scopeId(organizationId));

    assertEquals(workspaceInvitations.size(), workspaceResult.size());
    assertEquals(organizationInvitations.size(), organizationResult.size());

    verify(service, times(1)).getPendingInvitations(ScopeType.WORKSPACE, workspaceId);
    verify(service, times(1)).getPendingInvitations(ScopeType.ORGANIZATION, organizationId);

    verify(mapper, times(workspaceInvitations.size() + organizationInvitations.size())).toApi(any(UserInvitation.class));
  }

}
