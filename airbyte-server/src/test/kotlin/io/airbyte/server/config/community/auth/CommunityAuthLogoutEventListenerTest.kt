/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config.community.auth

import io.airbyte.api.problems.throwable.generated.UnprocessableEntityProblem
import io.airbyte.data.services.AuthRefreshTokenService
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.event.LogoutEvent
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.util.Locale
import java.util.UUID

@MicronautTest
class CommunityAuthLogoutEventListenerTest {
  private val refreshTokenService = mockk<AuthRefreshTokenService>(relaxed = true)
  private val listener = CommunityAuthLogoutEventListener(refreshTokenService)

  @Test
  fun `should revoke token on logout`() {
    val sessionId = UUID.randomUUID().toString()
    val authentication = mockk<Authentication>()

    every { authentication.attributes } returns mapOf(SESSION_ID to sessionId)

    listener.onApplicationEvent(LogoutEvent(authentication, null, Locale.getDefault()))

    verify { refreshTokenService.revokeAuthRefreshToken(sessionId) }
  }

  @Test
  fun `should throw exception if session ID is missing`() {
    val authentication = mockk<Authentication>()
    every { authentication.attributes } returns emptyMap()

    assertThrows(UnprocessableEntityProblem::class.java) {
      listener.onApplicationEvent(LogoutEvent(authentication, null, Locale.getDefault()))
    }
  }
}
