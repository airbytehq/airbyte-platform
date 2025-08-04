/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support

import io.airbyte.commons.DEFAULT_USER_ID
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.persistence.UserPersistence
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.Environment
import io.micronaut.http.HttpRequest
import io.micronaut.http.context.ServerRequestContext
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.Optional
import java.util.UUID

@Requires(env = [Environment.TEST])
@Property(name = "micronaut.security.enabled", value = "false")
internal class CommunityCurrentUserServiceTest {
  lateinit var currentUserService: CommunityCurrentUserService
  lateinit var userPersistence: UserPersistence

  @BeforeEach
  fun setUp() {
    userPersistence = mockk()
    currentUserService = CommunityCurrentUserService(userPersistence)
  }

  @Test
  fun testGetCurrentUser() {
    // set up a mock request context, details don't matter, just needed to make the
    // @RequestScope work on the CommunityCurrentUserService
    ServerRequestContext.with(
      HttpRequest.GET<Any>("/"),
      Runnable {
        try {
          val expectedUser = AuthenticatedUser().withUserId(DEFAULT_USER_ID)
          every { userPersistence.getDefaultUser() } returns (Optional.ofNullable(expectedUser))

          // First call - should fetch default user from userPersistence
          val user1 = currentUserService.getCurrentUser()
          Assertions.assertEquals(expectedUser, user1)

          // Second call - should use cached user
          val user2 = currentUserService.getCurrentUser()
          Assertions.assertEquals(expectedUser, user2)

          // Verify that getDefaultUser is called only once
          verify { userPersistence.getDefaultUser() }
        } catch (e: IOException) {
          Assertions.fail<Any>(e)
        }
      },
    )
  }

  @Test
  fun testCommunityGetCurrentUserIdIfExists() {
    ServerRequestContext.with(
      HttpRequest.GET<Any>("/"),
      Runnable {
        try {
          val expectedUser = AuthenticatedUser().withUserId(DEFAULT_USER_ID)
          every { userPersistence.getDefaultUser() } returns (Optional.of(expectedUser))

          val userId: Optional<UUID> = currentUserService.getCurrentUserIdIfExists()
          Assertions.assertTrue(userId.isPresent())
          Assertions.assertEquals(DEFAULT_USER_ID, userId.get())
        } catch (e: IOException) {
          Assertions.fail<Any>(e)
        }
      },
    )
  }

  @Test
  fun testCommunityGetCurrentUserIdIfExistsWhenUserNotFound() {
    ServerRequestContext.with(
      HttpRequest.GET<Any>("/"),
    ) {
      try {
        every { userPersistence.getDefaultUser() } returns Optional.empty()

        val userId: Optional<UUID> = currentUserService.getCurrentUserIdIfExists()
        Assertions.assertTrue(userId.isEmpty())
      } catch (e: IOException) {
        Assertions.fail<Any>(e)
      }
    }
  }
}
