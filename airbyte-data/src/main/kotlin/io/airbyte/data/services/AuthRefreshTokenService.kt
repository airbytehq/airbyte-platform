/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.AuthRefreshToken

/**
 * A service that manages auth_refresh_tokens.
 */
interface AuthRefreshTokenService {
  /**
   * Get an auth_refresh_token by its value.
   */
  fun getAuthRefreshToken(tokenValue: String): AuthRefreshToken?

  /**
   * Save a new auth_refresh_token with provided session ID and token value.
   */
  fun saveAuthRefreshToken(
    sessionId: String,
    tokenValue: String,
  ): AuthRefreshToken

  /**
   * Revoke all auth_refresh_tokens for a given session ID.
   */
  fun revokeAuthRefreshToken(sessionId: String): List<AuthRefreshToken>
}
