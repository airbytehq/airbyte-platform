/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.scim

import jakarta.inject.Singleton
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.security.SecureRandom
import java.util.HexFormat

@Singleton
open class ScimTokenService {
  open fun generateToken(): String {
    val randomBytes = ByteArray(TOKEN_RANDOM_BYTES)
    secureRandom.nextBytes(randomBytes)
    return TOKEN_PREFIX + HexFormat.of().formatHex(randomBytes)
  }

  open fun hashToken(rawToken: String): String {
    require(rawToken.isNotEmpty()) { "Token cannot be empty" }

    return try {
      val digest = MessageDigest.getInstance(SHA_256)
      HexFormat.of().formatHex(digest.digest(rawToken.toByteArray(StandardCharsets.UTF_8)))
    } catch (e: NoSuchAlgorithmException) {
      throw IllegalStateException("SHA-256 algorithm not available", e)
    }
  }

  private companion object {
    const val TOKEN_PREFIX = "airbyte_scim_"
    const val TOKEN_RANDOM_BYTES = 32
    const val SHA_256 = "SHA-256"
    val secureRandom = SecureRandom()
  }
}
