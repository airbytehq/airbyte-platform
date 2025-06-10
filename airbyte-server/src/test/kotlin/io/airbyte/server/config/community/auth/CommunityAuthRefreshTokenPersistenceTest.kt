/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config.community.auth

import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.DEFAULT_USER_ID
import io.airbyte.commons.auth.AuthRole
import io.airbyte.config.AuthRefreshToken
import io.airbyte.data.services.AuthRefreshTokenService
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.token.event.RefreshTokenGeneratedEvent
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.reactivestreams.Publisher
import reactor.test.StepVerifier

@MicronautTest
class CommunityAuthRefreshTokenPersistenceTest {
  private val authRefreshTokenService = mockk<AuthRefreshTokenService>(relaxed = true)
  private val refreshTokenPersistence = CommunityAuthRefreshTokenPersistence(authRefreshTokenService)

  @Test
  fun `should persist token successfully`() {
    val sessionId = "testSessionId"
    val refreshToken = "testRefreshToken"
    val event = mockk<RefreshTokenGeneratedEvent>()
    every { event.authentication.attributes } returns mapOf(SESSION_ID to sessionId)
    every { event.refreshToken } returns refreshToken

    refreshTokenPersistence.persistToken(event)

    verify { authRefreshTokenService.saveAuthRefreshToken(sessionId, refreshToken) }
  }

  @Test
  fun `should retrieve authentication for valid token`() {
    val refreshToken = "validToken"
    val sessionId = "testSessionId"
    every { authRefreshTokenService.getAuthRefreshToken(refreshToken) } returns
      AuthRefreshToken().withSessionId(sessionId).withValue(refreshToken).withRevoked(false)

    val result = refreshTokenPersistence.getAuthentication(refreshToken)

    StepVerifier
      .create(result)
      .assertNext { authentication ->
        assertEquals(DEFAULT_USER_ID.toString(), authentication.name)
        assertTrue(authentication.roles.containsAll(AuthRole.getInstanceAdminRoles()))
        assertEquals(sessionId, authentication.attributes[SESSION_ID])
      }.verifyComplete()
  }

  @Test
  fun `should throw ForbiddenProblem for revoked token`() {
    val refreshToken = "revokedToken"
    every { authRefreshTokenService.getAuthRefreshToken(refreshToken) } returns
      AuthRefreshToken().withSessionId("testSessionId").withValue(refreshToken).withRevoked(true)

    val result: Publisher<Authentication> = refreshTokenPersistence.getAuthentication(refreshToken)

    StepVerifier
      .create(result)
      .expectError(ForbiddenProblem::class.java)
      .verify()
  }
}
