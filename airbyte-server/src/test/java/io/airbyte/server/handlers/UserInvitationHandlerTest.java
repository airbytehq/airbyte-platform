/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers;

import static io.airbyte.config.Permission.PermissionType.WORKSPACE_ADMIN;
import static io.airbyte.server.handlers.UserInvitationHandler.ACCEPT_INVITE_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.PermissionCreate;
import io.airbyte.api.model.generated.PermissionType;
import io.airbyte.api.model.generated.UserInvitationCreateRequestBody;
import io.airbyte.api.model.generated.UserInvitationCreateResponse;
import io.airbyte.api.model.generated.UserInvitationListRequestBody;
import io.airbyte.api.model.generated.UserInvitationRead;
import io.airbyte.commons.server.handlers.PermissionHandler;
import io.airbyte.config.InvitationStatus;
import io.airbyte.config.ScopeType;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.User;
import io.airbyte.config.UserInvitation;
import io.airbyte.config.UserPermission;
import io.airbyte.config.persistence.PermissionPersistence;
import io.airbyte.config.persistence.UserPersistence;
import io.airbyte.data.services.OrganizationService;
import io.airbyte.data.services.UserInvitationService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.notification.CustomerIoEmailConfig;
import io.airbyte.notification.CustomerIoEmailNotificationSender;
import io.airbyte.persistence.job.WebUrlHelper;
import io.airbyte.server.handlers.api_domain_mapping.UserInvitationMapper;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class UserInvitationHandlerTest {

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

  UserInvitationHandler handler;

  @BeforeEach
  void setup() {
    handler = new UserInvitationHandler(service, mapper, customerIoEmailNotificationSender, webUrlHelper, workspaceService, organizationService,
        userPersistence, permissionPersistence, permissionHandler);
  }

  @Nested
  class CreateInvitationOrPermission {

    private static final User CURRENT_USER = new User().withUserId(UUID.randomUUID()).withEmail("current-user@airbyte.io").withName("Current User");
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

      @BeforeEach
      void setup() {
        when(webUrlHelper.getBaseUrl()).thenReturn(WEBAPP_BASE_URL);
        when(mapper.toDomain(USER_INVITATION_CREATE_REQUEST_BODY)).thenReturn(USER_INVITATION);
        when(service.createUserInvitation(USER_INVITATION)).thenReturn(USER_INVITATION);
      }

      @Test
      void testNewEmailWorkspaceInOrg() throws Exception {
        // the workspace is in an org.
        when(workspaceService.getOrganizationIdFromWorkspaceId(WORKSPACE_ID)).thenReturn(Optional.of(ORG_ID));

        // no existing user has the invited email.
        when(userPersistence.getUsersByEmail(INVITED_EMAIL)).thenReturn(Collections.emptyList());

        // call the handler method under test.
        final UserInvitationCreateResponse result = handler.createInvitationOrPermission(USER_INVITATION_CREATE_REQUEST_BODY, CURRENT_USER);

        // make sure correct invite was created, email was sent, and result is correct.
        verifyInvitationCreatedAndEmailSentResult(result);
      }

      @Test
      void testWorkspaceNotInAnyOrg() throws Exception {
        // the workspace is not in any org.
        when(workspaceService.getOrganizationIdFromWorkspaceId(WORKSPACE_ID)).thenReturn(Optional.empty());

        // call the handler method under test.
        final UserInvitationCreateResponse result = handler.createInvitationOrPermission(USER_INVITATION_CREATE_REQUEST_BODY, CURRENT_USER);

        // make sure correct invite was created, email was sent, and result is correct.
        verifyInvitationCreatedAndEmailSentResult(result);
      }

      @Test
      void testExistingEmailButNotInWorkspaceOrg() throws Exception {
        // the workspace is in an org.
        when(workspaceService.getOrganizationIdFromWorkspaceId(WORKSPACE_ID)).thenReturn(Optional.of(ORG_ID));

        // a user with the email exists, but is not in the workspace's org.
        final User userWithEmail = new User().withUserId(UUID.randomUUID()).withEmail(INVITED_EMAIL);
        when(userPersistence.getUsersByEmail(INVITED_EMAIL)).thenReturn(List.of(userWithEmail));

        // the org has a user with a different email, but not the one we're inviting.
        final User otherUserInOrg = new User().withUserId(UUID.randomUUID()).withEmail("other@airbyte.io");
        when(permissionPersistence.listUsersInOrganization(ORG_ID)).thenReturn(List.of(new UserPermission().withUser(otherUserInOrg)));

        // call the handler method under test.
        final UserInvitationCreateResponse result = handler.createInvitationOrPermission(USER_INVITATION_CREATE_REQUEST_BODY, CURRENT_USER);

        // make sure correct invite was created, email was sent, and result is correct.
        verifyInvitationCreatedAndEmailSentResult(result);
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

        // set up three users with the same email, two in the workspace's org and one outside of it.
        final User matchingUserInOrg1 = new User().withUserId(UUID.randomUUID()).withEmail(INVITED_EMAIL);
        final User matchingUserInOrg2 = new User().withUserId(UUID.randomUUID()).withEmail(INVITED_EMAIL);
        final User matchingUserNotInOrg = new User().withUserId(UUID.randomUUID()).withEmail(INVITED_EMAIL);
        when(userPersistence.getUsersByEmail(INVITED_EMAIL)).thenReturn(List.of(matchingUserInOrg1, matchingUserInOrg2, matchingUserNotInOrg));

        // set up three users inside the workspace's org, two with the same email and one with a different
        // email.
        final User otherUserInOrg = new User().withUserId(UUID.randomUUID()).withEmail("other@airbyte.io");
        when(permissionPersistence.listUsersInOrganization(ORG_ID)).thenReturn(List.of(
            new UserPermission().withUser(matchingUserInOrg1),
            new UserPermission().withUser(matchingUserInOrg2),
            new UserPermission().withUser(otherUserInOrg)));

        // call the handler method under test.
        final UserInvitationCreateResponse result = handler.createInvitationOrPermission(USER_INVITATION_CREATE_REQUEST_BODY, CURRENT_USER);

        // make sure permissions were created, appropriate email was sent, and result is correct.
        verifyPermissionAddedResult(Set.of(matchingUserInOrg1.getUserId(), matchingUserInOrg2.getUserId()), result);
      }

    }

    private void verifyPermissionAdded(final UUID userId) throws Exception {
      // capture and verify the permission that is created by the permission handler.
      final ArgumentCaptor<PermissionCreate> permissionCreateCaptor = ArgumentCaptor.forClass(PermissionCreate.class);
      verify(permissionHandler, times(1)).createPermission(permissionCreateCaptor.capture());
      final PermissionCreate capturedPermissionCreate = permissionCreateCaptor.getValue();

      assertEquals(userId, capturedPermissionCreate.getUserId());
      assertEquals(WORKSPACE_ID, capturedPermissionCreate.getWorkspaceId());
      assertEquals(PermissionType.WORKSPACE_ADMIN, capturedPermissionCreate.getPermissionType());
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
