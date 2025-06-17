/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.auth

import io.airbyte.data.TokenType

/**
 * AirbyteJwtGenerator provides an interface for generating an encoded JWT token.
 * When Micronaut auth is enabled, this will be a signed JWT.
 * When Micronaut auth is disabled, this will be an unsigned JWT.
 */
interface AirbyteJwtGenerator {
  fun generateToken(
    tokenSubject: String,
    tokenType: TokenType,
    tokenExpirationLength: Long,
    additionalClaims: Map<String, Any> = emptyMap(),
  ): String
}
