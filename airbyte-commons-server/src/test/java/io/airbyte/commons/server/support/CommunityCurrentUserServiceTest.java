/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import static io.airbyte.commons.ConstantsKt.DEFAULT_USER_ID;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.config.AuthenticatedUser;
import io.airbyte.config.persistence.UserPersistence;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.Environment;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.context.ServerRequestContext;
import io.micronaut.test.annotation.MockBean;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@MicronautTest
@Requires(env = {Environment.TEST})
@Property(name = "micronaut.security.enabled",
          value = "false")
@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class CommunityCurrentUserServiceTest {

  @MockBean(UserPersistence.class)
  UserPersistence mockUserPersistence() {
    return Mockito.mock(UserPersistence.class);
  }

  @Inject
  CommunityCurrentUserService currentUserService;

  @Inject
  UserPersistence userPersistence;

  @Test
  void testGetCurrentUser() {
    // set up a mock request context, details don't matter, just needed to make the
    // @RequestScope work on the CommunityCurrentUserService
    ServerRequestContext.with(HttpRequest.GET("/"), () -> {
      try {
        final AuthenticatedUser expectedUser = new AuthenticatedUser().withUserId(DEFAULT_USER_ID);
        when(userPersistence.getDefaultUser()).thenReturn(Optional.ofNullable(expectedUser));

        // First call - should fetch default user from userPersistence
        final AuthenticatedUser user1 = currentUserService.getCurrentUser();
        Assertions.assertEquals(expectedUser, user1);

        // Second call - should use cached user
        final AuthenticatedUser user2 = currentUserService.getCurrentUser();
        Assertions.assertEquals(expectedUser, user2);

        // Verify that getDefaultUser is called only once
        verify(userPersistence, times(1)).getDefaultUser();
      } catch (final IOException e) {
        fail(e);
      }
    });
  }

  @Test
  void testCommunityGetCurrentUserIdIfExists() {
    ServerRequestContext.with(HttpRequest.GET("/"), () -> {
      try {
        final AuthenticatedUser expectedUser = new AuthenticatedUser().withUserId(DEFAULT_USER_ID);
        when(userPersistence.getDefaultUser()).thenReturn(Optional.of(expectedUser));

        final Optional<UUID> userId = currentUserService.getCurrentUserIdIfExists();
        Assertions.assertTrue(userId.isPresent());
        Assertions.assertEquals(DEFAULT_USER_ID, userId.get());
      } catch (final IOException e) {
        fail(e);
      }
    });
  }

  @Test
  void testCommunityGetCurrentUserIdIfExistsWhenUserNotFound() {
    ServerRequestContext.with(HttpRequest.GET("/"), () -> {
      try {
        when(userPersistence.getDefaultUser()).thenReturn(Optional.empty());

        final Optional<UUID> userId = currentUserService.getCurrentUserIdIfExists();
        Assertions.assertTrue(userId.isEmpty());
      } catch (final IOException e) {
        fail(e);
      }
    });
  }

}
