/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.auth

import io.airbyte.commons.json.Jsons
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.time.Clock
import java.time.Instant
import java.util.Base64

@Singleton
@Requires(property = "micronaut.security.enabled", notEquals = "true")
class AirbyteJwtGeneratorNoAuthImpl : AirbyteJwtGenerator {
  var clock: Clock = Clock.systemUTC()

  override fun generateToken(
    tokenSubject: String,
    tokenType: String,
    tokenExpirationLength: Long,
    additionalClaims: Map<String, Any>,
  ): String {
    val header = """{"alg":"none"}"""
    val exp = Instant.now(clock).plusSeconds(tokenExpirationLength).epochSecond
    val claims =
      mutableMapOf<String, Any>(
        "sub" to tokenSubject,
        "typ" to tokenType,
        "exp" to exp,
      )

    for ((key, value) in additionalClaims) {
      if (!claims.containsKey(key)) {
        claims[key] = value
      }
    }

    val payload = Jsons.serialize(claims)
    val emptySignature = ""

    val encoder = Base64.getUrlEncoder().withoutPadding()
    val encodedHeader = encoder.encodeToString(header.toByteArray())
    val encodedPayload = encoder.encodeToString(payload.toByteArray())

    return "$encodedHeader.$encodedPayload.$emptySignature"
  }
}
