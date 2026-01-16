/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.repositories.AuthRefreshTokenRepository
import io.airbyte.data.repositories.entities.AuthRefreshToken
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val TOKEN_VALUE = "tokenValue"
private const val SESSION_ID = "sessionId"

internal class AuthRefreshTokenServiceDataImplTest {
  private val authRefreshTokenRepository = mockk<AuthRefreshTokenRepository>()
  private val authRefreshTokenServiceDataImpl = AuthRefreshTokenServiceDataImpl(authRefreshTokenRepository)

  @BeforeEach
  fun reset() {
    clearAllMocks()
  }

  @Test
  fun `test getAuthRefreshToken`() {
    val authRefreshToken =
      AuthRefreshToken(
        sessionId = SESSION_ID,
        value = TOKEN_VALUE,
        revoked = false,
      )
    every { authRefreshTokenRepository.findByValue(TOKEN_VALUE) } returns authRefreshToken

    val result = authRefreshTokenServiceDataImpl.getAuthRefreshToken(TOKEN_VALUE)

    val expectedAuthRefreshToken =
      io.airbyte.config
        .AuthRefreshToken()
        .withSessionId(SESSION_ID)
        .withValue(TOKEN_VALUE)
        .withRevoked(false)

    assert(result == expectedAuthRefreshToken)
  }

  @Test
  fun `test saveAuthRefreshToken`() {
    val authRefreshToken =
      AuthRefreshToken(
        sessionId = SESSION_ID,
        value = TOKEN_VALUE,
        revoked = false,
      )
    every { authRefreshTokenRepository.save(authRefreshToken) } returns authRefreshToken

    val result = authRefreshTokenServiceDataImpl.saveAuthRefreshToken(SESSION_ID, TOKEN_VALUE)

    val expectedAuthRefreshToken =
      io.airbyte.config
        .AuthRefreshToken()
        .withSessionId(SESSION_ID)
        .withValue(TOKEN_VALUE)
        .withRevoked(false)

    assert(result == expectedAuthRefreshToken)
  }

  @Test
  fun `test revokeAuthRefreshToken`() {
    val authRefreshToken1 =
      AuthRefreshToken(
        sessionId = SESSION_ID,
        value = TOKEN_VALUE,
        revoked = false,
      )
    val authRefreshToken2 =
      AuthRefreshToken(
        sessionId = SESSION_ID,
        value = "someOtherValueForSameSession",
        revoked = false,
      )
    every { authRefreshTokenRepository.findBySessionId(SESSION_ID) } returns listOf(authRefreshToken1, authRefreshToken2)
    every {
      authRefreshTokenRepository.updateAll(
        listOf(authRefreshToken1.apply { revoked = true }, authRefreshToken2.apply { revoked = true }),
      )
    } returns
      listOf(
        authRefreshToken1.apply { revoked = true },
        authRefreshToken2.apply { revoked = true },
      )

    val result = authRefreshTokenServiceDataImpl.revokeAuthRefreshToken(SESSION_ID)

    verify {
      authRefreshTokenRepository.updateAll(
        listOf(authRefreshToken1.apply { revoked = true }, authRefreshToken2.apply { revoked = true }),
      )
    }

    val expectedAuthRefreshToken1 =
      io.airbyte.config
        .AuthRefreshToken()
        .withSessionId(SESSION_ID)
        .withValue(TOKEN_VALUE)
        .withRevoked(true)

    val expectedAuthRefreshToken2 =
      io.airbyte.config
        .AuthRefreshToken()
        .withSessionId(SESSION_ID)
        .withValue("someOtherValueForSameSession")
        .withRevoked(true)

    assert(result.size == 2)
    assert(result[0] == expectedAuthRefreshToken1)
    assert(result[1] == expectedAuthRefreshToken2)
  }
}
