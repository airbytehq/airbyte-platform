/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support

import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.persistence.UserPersistence
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.Environment
import io.micronaut.http.HttpRequest
import io.micronaut.http.context.ServerRequestContext
import io.micronaut.security.utils.SecurityService
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.mockito.Mockito
import java.io.IOException
import java.util.Optional
import java.util.UUID

@MicronautTest
@Requires(env = [Environment.TEST])
@Property(name = "micronaut.security.enabled", value = "true")
internal class SecurityAwareCurrentUserServiceTest {
  @MockBean(SecurityService::class)
  fun mockSecurityService(): SecurityService = Mockito.mock(SecurityService::class.java)

  @MockBean(UserPersistence::class)
  fun mockUserPersistence(): UserPersistence = Mockito.mock(UserPersistence::class.java)

  @Inject
  lateinit var currentUserService: SecurityAwareCurrentUserService

  @Inject
  lateinit var securityService: SecurityService

  @Inject
  lateinit var userPersistence: UserPersistence

  // todo (cgardens) fix in commons-server PR
  @Disabled
  @Test
  fun testGetCurrentUser() {
    // set up a mock request context, details don't matter, just needed to make the
    // @RequestScope work on the SecurityAwareCurrentUserService
    ServerRequestContext.with(
      HttpRequest.GET<Any>("/"),
      Runnable {
        try {
          val authUserId = "testUser"
          val expectedUser = AuthenticatedUser().withAuthUserId(authUserId)

          Mockito.`when`(securityService.username()).thenReturn(Optional.of(authUserId))
          Mockito
            .`when`(userPersistence.getUserByAuthId(authUserId))
            .thenReturn(Optional.of(expectedUser))

          // First call - should fetch from userPersistence
          val user1 = currentUserService.getCurrentUser()
          Assertions.assertEquals(expectedUser, user1)

          // Second call - should use cached user
          val user2 = currentUserService.getCurrentUser()
          Assertions.assertEquals(expectedUser, user2)

          // Verify that getUserByAuthId is called only once
          Mockito.verify(userPersistence, Mockito.times(1)).getUserByAuthId(authUserId)
        } catch (e: IOException) {
          Assertions.fail<Any>(e)
        }
      },
    )
  }

  // todo (cgardens) fix in commons-server PR
  @Disabled
  @Test
  fun testGetCurrentUserIdIfExistsReturnsCorrectId() {
    ServerRequestContext.with(
      HttpRequest.GET<Any>("/"),
      Runnable {
        try {
          val userId = UUID.randomUUID()
          val authUserId = "123e4567-e89b-12d3-a456-426614174000"
          val expectedUser = AuthenticatedUser().withAuthUserId(authUserId).withUserId(userId)

          Mockito.`when`(securityService.username()).thenReturn(Optional.of(authUserId))
          Mockito
            .`when`(userPersistence.getUserByAuthId(authUserId))
            .thenReturn(Optional.of(expectedUser))

          val retrievedUserId: Optional<UUID> = currentUserService.getCurrentUserIdIfExists()
          Assertions.assertTrue(retrievedUserId.isPresent())
          Assertions.assertEquals(userId, retrievedUserId.get())
        } catch (e: IOException) {
          Assertions.fail<Any>(e)
        }
      },
    )
  }

  @Test
  fun testGetCurrentUserIdIfExistsDoesNotThrowWhenNoUser() {
    ServerRequestContext.with(
      HttpRequest.GET<Any>("/"),
      Runnable {
        Assertions.assertDoesNotThrow(
          Executable {
            Mockito.`when`(securityService.username()).thenReturn(Optional.empty())
            val userId: Optional<UUID> = currentUserService.getCurrentUserIdIfExists()
            Assertions.assertTrue(userId.isEmpty())
          },
        )
      },
    )
  }
}
