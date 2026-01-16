/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.AuthRefreshToken
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

private const val TOKEN_VALUE_1 = "tokenValue1"
private const val TOKEN_VALUE_2 = "tokenValue2"
private const val SESSION_ID_1 = "sessionId1"

@MicronautTest
internal class AuthRefreshTokenRepositoryTest : AbstractConfigRepositoryTest() {
  @AfterEach
  fun tearDown() {
    authRefreshTokenRepository.deleteAll()
  }

  @Test
  fun `test db insertion and retrieval`() {
    val authRefreshToken =
      AuthRefreshToken(
        sessionId = SESSION_ID_1,
        value = TOKEN_VALUE_1,
        revoked = false,
      )

    val countBeforeSave = authRefreshTokenRepository.count()
    assert(countBeforeSave == 0L)

    val saveResult = authRefreshTokenRepository.save(authRefreshToken)
    val countAfterSave = authRefreshTokenRepository.count()
    assert(countAfterSave == 1L)

    val persistedAuthRefreshToken = authRefreshTokenRepository.findByValue(saveResult.value)!!
    assert(persistedAuthRefreshToken.value == TOKEN_VALUE_1)
    assert(persistedAuthRefreshToken.sessionId == SESSION_ID_1)
    assert(!persistedAuthRefreshToken.revoked)
  }

  @Test
  fun `test find by sessionId`() {
    assert(authRefreshTokenRepository.findBySessionId(SESSION_ID_1).isEmpty())

    val authRefreshToken1 =
      AuthRefreshToken(
        sessionId = SESSION_ID_1,
        value = TOKEN_VALUE_1,
        revoked = false,
      )

    val authRefreshToken2 =
      AuthRefreshToken(
        sessionId = SESSION_ID_1,
        value = TOKEN_VALUE_2,
        revoked = true,
      )

    val authRefreshTokenOtherSession =
      AuthRefreshToken(
        sessionId = "otherSession",
        value = "otherTokenValue",
        revoked = false,
      )

    authRefreshTokenRepository.saveAll(listOf(authRefreshToken1, authRefreshToken2, authRefreshTokenOtherSession))

    val tokens = authRefreshTokenRepository.findBySessionId(SESSION_ID_1).map { it.value }
    assert(tokens.size == 2)
    assert(tokens.contains(authRefreshToken1.value))
    assert(tokens.contains(authRefreshToken2.value))
  }
}
