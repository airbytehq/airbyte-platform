/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.auth

import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.micronaut.security.token.jwt.generator.JwtTokenGenerator
import jakarta.inject.Singleton
import java.time.Clock
import java.time.temporal.ChronoUnit

private const val AIRBYTE_SERVER_AUDIENCE = "airbyte-server"

@Singleton
@Requires(property = "micronaut.security.enabled", value = "true")
@Requires(property = "micronaut.security.token.jwt.enabled", value = "true")
@Replaces(AirbyteJwtGeneratorNoAuthImpl::class)
class AirbyteJwtGeneratorImpl(
  private val jwtGenerator: JwtTokenGenerator,
  @Property(name = "airbyte.auth.token-issuer") private val tokenIssuer: String = "airbyte",
) : AirbyteJwtGenerator {
  var clock: Clock = Clock.systemUTC()

  override fun generateToken(
    tokenSubject: String,
    tokenType: String, // TODO (cjkenned): this should be a TOKEN_TYPE enum, when available
    tokenExpirationLength: Long,
    additionalClaims: Map<String, Any>,
  ): String {
    val claims: MutableMap<String, Any> =
      mutableMapOf(
        "iss" to tokenIssuer,
        "aud" to AIRBYTE_SERVER_AUDIENCE,
        "sub" to tokenSubject,
        "typ" to tokenType,
        "exp" to clock.instant().plus(tokenExpirationLength, ChronoUnit.MINUTES).epochSecond,
      )

    for ((key, value) in additionalClaims) {
      if (!claims.containsKey(key)) {
        claims[key] = value
      }
    }

    return jwtGenerator.generateToken(claims).orElseThrow {
      AirbyteJwtTokenGenerationError()
    }
  }
}

class AirbyteJwtTokenGenerationError : Exception("Error generating JWT token")
