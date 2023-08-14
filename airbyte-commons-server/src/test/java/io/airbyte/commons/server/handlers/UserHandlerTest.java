/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.UserCreate;
import io.airbyte.api.model.generated.UserRead;
import io.airbyte.api.model.generated.UserStatus;
import io.airbyte.config.User;
import io.airbyte.config.User.AuthProvider;
import io.airbyte.config.User.Status;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.UserPersistence;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UserHandlerTest {

  private Supplier<UUID> uuidSupplier;
  private UserHandler userHandler;
  private UserPersistence userPersistence;

  private final UUID userId = UUID.randomUUID();
  private final String userName = "user 1";
  private final String userEmail = "user_1@whatever.com";

  private final User user = new User()
      .withUserId(userId)
      .withAuthUserId(userId.toString())
      .withEmail(userEmail)
      .withAuthProvider(AuthProvider.GOOGLE_IDENTITY_PLATFORM)
      .withStatus(Status.INVITED)
      .withName(userName);

  @BeforeEach
  void setUp() {
    userPersistence = mock(UserPersistence.class);
    uuidSupplier = mock(Supplier.class);
    userHandler = new UserHandler(userPersistence, uuidSupplier);
  }

  @Test
  void testCreateUser() throws JsonValidationException, ConfigNotFoundException, IOException {
    when(uuidSupplier.get()).thenReturn(userId);
    when(userPersistence.getUser(any())).thenReturn(Optional.of(user));
    final UserCreate userCreate = new UserCreate()
        .name(userName)
        .authUserId(userId.toString())
        .authProvider(
            io.airbyte.api.model.generated.AuthProvider.GOOGLE_IDENTITY_PLATFORM)
        .status(UserStatus.DISABLED.INVITED)
        .email(userEmail);
    final UserRead actualRead = userHandler.createUser(userCreate);
    final UserRead expectedRead = new UserRead()
        .userId(userId)
        .name(userName)
        .authUserId(userId.toString())
        .authProvider(
            io.airbyte.api.model.generated.AuthProvider.GOOGLE_IDENTITY_PLATFORM)
        .status(UserStatus.DISABLED.INVITED)
        .email(userEmail)
        .companyName(null)
        .metadata(null)
        .news(false);

    assertEquals(expectedRead, actualRead);
  }

}
