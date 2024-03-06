/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mock.Strictness.LENIENT;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.UserInvitationCreateRequestBody;
import io.airbyte.api.model.generated.UserInvitationRead;
import io.airbyte.config.InvitationStatus;
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
import java.util.UUID;
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

}
