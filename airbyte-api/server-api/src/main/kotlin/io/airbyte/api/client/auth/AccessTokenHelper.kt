/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.auth

import com.fasterxml.jackson.databind.ObjectMapper
import java.time.Instant
import java.util.Base64

class AccessTokenHelper {
  companion object {
    private const val EXPIRATION_CLAIM = "exp"

    private val objectMapper = ObjectMapper()

    /**
     * Decodes a JWT token (in String form) and extracts the 'exp' claim as an [Instant].
     *
     * @throws Exception if the token is not properly formatted or the expiration cannot be extracted.
     */
    fun decodeExpiry(token: String): Instant {
      val parts = token.split(".")
      if (parts.size < 2) {
        throw IllegalStateException("Invalid JWT token format")
      }
      return try {
        val payloadJson = String(Base64.getUrlDecoder().decode(parts[1]))
        val exp = objectMapper.readTree(payloadJson).get(EXPIRATION_CLAIM).asLong()
        Instant.ofEpochSecond(exp)
      } catch (e: Exception) {
        throw Exception("Failed to extract expiration from JWT token payload", e)
      }
    }
  }
}
