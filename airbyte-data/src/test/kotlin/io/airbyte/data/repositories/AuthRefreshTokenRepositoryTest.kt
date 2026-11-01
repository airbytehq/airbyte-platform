/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.AuthRefreshToken
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
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
    assertEquals(0L, countBeforeSave)

    val saveResult = authRefreshTokenRepository.save(authRefreshToken)
    val countAfterSave = authRefreshTokenRepository.count()
    assertEquals(1L, countAfterSave)

    val persistedAuthRefreshToken = authRefreshTokenRepository.findByValue(saveResult.value)!!
    assertEquals(TOKEN_VALUE_1, persistedAuthRefreshToken.value)
    assertEquals(SESSION_ID_1, persistedAuthRefreshToken.sessionId)
    assertFalse(persistedAuthRefreshToken.revoked)
  }

  @Test
  fun `test find by sessionId`() {
    assertTrue(authRefreshTokenRepository.findBySessionId(SESSION_ID_1).isEmpty())

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
    assertEquals(2, tokens.size)
    assertTrue(tokens.contains(authRefreshToken1.value))
    assertTrue(tokens.contains(authRefreshToken2.value))
  }
}
