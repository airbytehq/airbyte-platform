/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mock.Strictness.LENIENT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.UserInvitationCreateRequestBody;
import io.airbyte.api.model.generated.UserInvitationListRequestBody;
import io.airbyte.api.model.generated.UserInvitationRead;
import io.airbyte.config.InvitationStatus;
import io.airbyte.config.ScopeType;
import io.airbyte.config.User;
import io.airbyte.config.UserInvitation;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.data.services.UserInvitationService;
import io.airbyte.notification.CustomerIoEmailConfig;
import io.airbyte.notification.CustomerIoEmailNotificationSender;
import io.airbyte.persistence.job.WebUrlHelper;
import io.airbyte.server.handlers.api_domain_mapping.UserInvitationMapper;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class UserInvitationHandlerTest {

  @Mock(strictness = LENIENT)
  UserInvitationService service;

  @Mock(strictness = LENIENT)
  UserInvitationMapper mapper;

  @Mock(strictness = LENIENT)
  CustomerIoEmailNotificationSender customerIoEmailNotificationSender;

  @Mock(strictness = LENIENT)
  WebUrlHelper webUrlHelper;

  UserInvitationHandler handler;

  @BeforeEach
  void setup() {
    handler = new UserInvitationHandler(service, mapper, customerIoEmailNotificationSender, webUrlHelper);
  }

  @Test
  public void testCreateUserInvitation() throws JsonValidationException, ConfigNotFoundException, IOException {
    // mocked data
    UserInvitationCreateRequestBody req = new UserInvitationCreateRequestBody();
    req.setInvitedEmail("test@example.com");
    User currentUser = new User();
    UUID currentUserId = UUID.randomUUID();
    currentUser.setUserId(currentUserId);
    currentUser.setName("inviterName");
    UserInvitation saved = new UserInvitation();
    saved.setInviteCode("randomCode");
    saved.setInviterUserId(currentUserId);
    saved.setStatus(InvitationStatus.PENDING);
    UserInvitationRead expected = new UserInvitationRead();
    expected.setInviteCode(saved.getInviteCode());
    expected.setInvitedEmail(req.getInvitedEmail());
    expected.setInviterUserId(currentUserId);

    when(mapper.toDomain(req)).thenReturn(new UserInvitation());
    when(service.createUserInvitation(any(UserInvitation.class))).thenReturn(saved);
    when(webUrlHelper.getBaseUrl()).thenReturn("cloud.airbyte.com");
    when(mapper.toApi(saved)).thenReturn(expected);

    final UserInvitationRead result = handler.create(req, currentUser);

    verify(mapper, times(1)).toDomain(req);
    verify(service, times(1)).createUserInvitation(any(UserInvitation.class));
    verify(webUrlHelper, times(1)).getBaseUrl();
    verify(customerIoEmailNotificationSender, times(1))
        .sendInviteToUser(any(CustomerIoEmailConfig.class), anyString(), anyString());

    assert result != null;
    assert result.equals(expected);
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

    Assertions.assertEquals(workspaceInvitations.size(), workspaceResult.size());
    Assertions.assertEquals(organizationInvitations.size(), organizationResult.size());

    verify(service, times(1)).getPendingInvitations(ScopeType.WORKSPACE, workspaceId);
    verify(service, times(1)).getPendingInvitations(ScopeType.ORGANIZATION, organizationId);

    verify(mapper, times(workspaceInvitations.size() + organizationInvitations.size())).toApi(any(UserInvitation.class));
  }

}
