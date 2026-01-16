/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.AuthRefreshToken
import io.airbyte.data.repositories.AuthRefreshTokenRepository
import io.airbyte.data.services.AuthRefreshTokenService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import jakarta.inject.Singleton

@Singleton
class AuthRefreshTokenServiceDataImpl(
  val authRefreshTokenRepository: AuthRefreshTokenRepository,
) : AuthRefreshTokenService {
  override fun getAuthRefreshToken(tokenValue: String): AuthRefreshToken? = authRefreshTokenRepository.findByValue(tokenValue)?.toConfigModel()

  override fun saveAuthRefreshToken(
    sessionId: String,
    tokenValue: String,
  ): AuthRefreshToken =
    authRefreshTokenRepository
      .save(
        io.airbyte.data.repositories.entities.AuthRefreshToken(
          sessionId = sessionId,
          value = tokenValue,
          revoked = false,
        ),
      ).toConfigModel()

  override fun revokeAuthRefreshToken(sessionId: String): List<AuthRefreshToken> {
    val tokens = authRefreshTokenRepository.findBySessionId(sessionId)
    tokens.forEach { it.revoked = true }
    authRefreshTokenRepository.updateAll(tokens)
    return tokens.map { it.toConfigModel() }
  }
}
