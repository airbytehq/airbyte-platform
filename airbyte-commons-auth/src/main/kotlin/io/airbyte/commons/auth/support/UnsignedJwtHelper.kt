/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.support

import java.time.Clock
import java.time.Instant
import java.util.Base64

/**
 * Helpers for building unsigned JWTs.
 */
object UnsignedJwtHelper {
  /**
   * Builds an unsigned JWT with a specified expiration claim.
   */
  fun buildUnsignedJwtWithExpClaim(
    secondsFromNow: Long,
    clock: Clock = Clock.systemUTC(),
  ): String {
    val header = """{"alg":"none"}"""
    // Set the expiration time to secondsFromNow seconds in the future.
    val exp = Instant.now(clock).plusSeconds(secondsFromNow).epochSecond
    // Define a payload with the new exp claim.
    val payload = """{"exp":$exp}"""
    val emptySignature = ""

    val encoder = Base64.getUrlEncoder().withoutPadding()
    val encodedHeader = encoder.encodeToString(header.toByteArray())
    val encodedPayload = encoder.encodeToString(payload.toByteArray())

    return "$encodedHeader.$encodedPayload.$emptySignature"
  }
}
