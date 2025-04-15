/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.helpers

import jakarta.inject.Singleton
import org.springframework.security.crypto.password.PasswordEncoder
import org.springframework.security.crypto.password.Pbkdf2PasswordEncoder

/**
 * A [PasswordEncoder] implementation that delegates to a PBKDF2-based encoder
 * provided by [Pbkdf2PasswordEncoder.defaultsForSpringSecurity_v5_8]. This
 * allows secure hashing and verification of raw passwords.
 */
@Singleton
class DataplanePasswordEncoder : PasswordEncoder {
  private val delegate: PasswordEncoder = Pbkdf2PasswordEncoder.defaultsForSpringSecurity_v5_8()

  /**
   * Encodes the given [rawPassword] using the delegated PBKDF2 encoder.
   *
   * @param rawPassword The raw password string to be hashed.
   * @return The resulting hashed password as a [String].
   */
  override fun encode(rawPassword: CharSequence): String = delegate.encode(rawPassword)

  /**
   * Validates that the provided [rawPassword] matches the given [encodedPassword].
   *
   * @param rawPassword The raw password to test.
   * @param encodedPassword The previously hashed password to compare against.
   * @return `true` if the raw and encoded passwords match, `false` otherwise.
   */
  override fun matches(
    rawPassword: CharSequence,
    encodedPassword: String,
  ): Boolean = delegate.matches(rawPassword, encodedPassword)
}
